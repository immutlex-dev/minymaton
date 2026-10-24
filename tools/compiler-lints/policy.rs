use std::collections::{HashMap, HashSet};

use rustc_ast::LitKind;
use rustc_hir::attrs::AttributeKind;
use rustc_hir::def::{DefKind, Res};
use rustc_hir::def_id::DefId;
use rustc_hir::intravisit::{self, Visitor};
use rustc_hir::{self as hir, Expr, ExprKind, HirId};
use rustc_infer::infer::TyCtxtInferExt;
use rustc_middle::ty::{self, TyCtxt};
use rustc_span::{Span, Symbol};
use rustc_trait_selection::traits::type_known_to_meet_bound_modulo_regions;

const CLOCK: u8 = 1;
const OBSERVATION: u8 = 2;
const EXPIRY: u8 = 4;
const REQUEST_BUDGET: u8 = 8;

struct RequestProvenance<'a, 'tcx> {
    tcx: TyCtxt<'tcx>,
    facts: &'a Facts,
    seen: HashSet<HirId>,
    valid: bool,
}

impl<'tcx> Visitor<'tcx> for RequestProvenance<'_, 'tcx> {
    fn visit_expr(&mut self, expression: &'tcx Expr<'tcx>) {
        if self.facts.get(expression) & REQUEST_BUDGET == 0 || !self.seen.insert(expression.hir_id)
        {
            return;
        }
        if in_loop(self.tcx, expression.hir_id) {
            self.valid = false;
        }
        if let Some(place) = Place::of(self.tcx, expression) {
            if self.facts.mutations.contains(&place.binding) {
                self.valid = false;
            }
            for value in self.facts.values.get(&place.binding).into_iter().flatten() {
                if let hir::Node::Expr(value) = self.tcx.hir_node(*value) {
                    self.visit_expr(value);
                }
            }
        }
        if let ExprKind::Binary(op, left, right) = expression.kind {
            let valid_add = op.node == hir::BinOpKind::Add
                && ((self.facts.plain_clock(self.tcx, left, &mut HashSet::new())
                    && self.facts.get(left) & REQUEST_BUDGET == 0
                    && duration_type(self.tcx, right)
                    && self.facts.get(right) & REQUEST_BUDGET != 0)
                    || (self.facts.plain_clock(self.tcx, right, &mut HashSet::new())
                        && self.facts.get(right) & REQUEST_BUDGET == 0
                        && duration_type(self.tcx, left)
                        && self.facts.get(left) & REQUEST_BUDGET != 0));
            if !valid_add {
                self.valid = false;
            }
        }
        if let ExprKind::Call(_, [input]) = expression.kind
            && resolved(self.tcx, expression).is_some_and(|id| {
                self.tcx.crate_name(id.krate).as_str() == "serde_json"
                    && self.tcx.item_name(id).as_str() == "from_value"
            })
            && !self
                .facts
                .parameter_origin(self.tcx, input, &mut HashSet::new())
        {
            self.valid = false;
        }
        intravisit::walk_expr(self, expression);
    }
}

fn normalized(name: &str) -> String {
    name.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn contract(name: &str) -> bool {
    let characters: Vec<_> = name.chars().collect();
    let mut word = String::new();
    let mut words = Vec::new();
    for (index, character) in characters.iter().copied().enumerate() {
        let boundary = !character.is_ascii_alphanumeric()
            || (character.is_ascii_uppercase()
                && index != 0
                && (characters[index - 1].is_ascii_lowercase()
                    || characters
                        .get(index + 1)
                        .is_some_and(char::is_ascii_lowercase)));
        if boundary && !word.is_empty() {
            words.push(std::mem::take(&mut word));
        }
        if character.is_ascii_alphanumeric() {
            word.push(character.to_ascii_lowercase());
        }
    }
    words.push(word);
    words
        .iter()
        .any(|word| word == "schema" || word == "format" || word == "protocol")
}

fn generation_field(name: &str) -> bool {
    let name = normalized(name);
    name.strip_suffix("version")
        .or_else(|| name.strip_suffix("generation"))
        .is_some_and(|stem| {
            stem.ends_with("schema") || stem.ends_with("format") || stem.ends_with("protocol")
        })
}

fn numbered_generation(name: &str) -> bool {
    name.strip_prefix('V')
        .or_else(|| name.strip_prefix('v'))
        .is_some_and(|number| {
            !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn generation_literal(value: &str) -> bool {
    (value.starts_with('/') && value.split('/').any(numbered_generation))
        || value
            .trim_end_matches('\0')
            .rsplit(['-', '_'])
            .next()
            .is_some_and(|suffix| suffix != value && numbered_generation(suffix))
}

fn error(tcx: TyCtxt<'_>, span: Span, rule: &str, message: &str) {
    tcx.dcx()
        .span_err(span, format!("fogscrib::{rule}: {message}"));
}

// Anonymous constants share an HIR item owner but have independent type tables.
fn body_types<'tcx>(tcx: TyCtxt<'tcx>, node: HirId) -> &'tcx ty::TypeckResults<'tcx> {
    tcx.typeck(tcx.hir_enclosing_body_owner(node))
}

fn resolved(tcx: TyCtxt<'_>, expression: &Expr<'_>) -> Option<DefId> {
    let typeck = body_types(tcx, expression.hir_id);
    match expression.kind {
        ExprKind::Path(ref path) => typeck
            .qpath_res(path, expression.hir_id)
            .opt_def_id()
            .or_else(|| match typeck.expr_ty(expression).kind() {
                ty::FnDef(id, _) => Some(*id),
                _ => None,
            }),
        ExprKind::MethodCall(..) => typeck.type_dependent_def_id(expression.hir_id),
        ExprKind::Call(callee, _) => resolved(tcx, callee),
        _ => None,
    }
}

fn local_binding(tcx: TyCtxt<'_>, expression: &Expr<'_>) -> Option<HirId> {
    if let ExprKind::Path(ref path) = expression.kind
        && let Res::Local(id) =
            body_types(tcx, expression.hir_id).qpath_res(path, expression.hir_id)
    {
        Some(id)
    } else {
        None
    }
}

fn native_method(tcx: TyCtxt<'_>, id: DefId, owner: &str, method: &str) -> bool {
    if id.is_local() || tcx.def_kind(id) != DefKind::AssocFn || tcx.item_name(id).as_str() != method
    {
        return false;
    }
    let signature = tcx.fn_sig(id).instantiate_identity().skip_binder();
    matches!(signature.inputs().first().map(|receiver| receiver.peel_refs().kind()),
        Some(ty::Adt(definition, _))
            if definition.did().krate == id.krate && tcx.def_path_str(definition.did()) == owner)
}

fn pipewire_dispatch(tcx: TyCtxt<'_>, id: DefId) -> bool {
    let name = tcx.item_name(id);
    matches!(name.as_str(), "iterate" | "iterate_unguarded")
        && native_method(tcx, id, "pipewire::loop_::Loop", name.as_str())
}

fn pipewire_timeout(tcx: TyCtxt<'_>, expression: &Expr<'_>, name: &str) -> bool {
    let Some(id) = resolved(tcx, expression) else {
        return false;
    };
    let variant = if matches!(tcx.def_kind(id), DefKind::Ctor(..)) {
        tcx.parent(id)
    } else {
        id
    };
    !variant.is_local()
        && tcx.def_kind(variant) == DefKind::Variant
        && tcx.item_name(variant).as_str() == name
        && tcx.def_path_str(tcx.parent(variant)) == "pipewire::loop_::Timeout"
        && matches!(body_types(tcx, expression.hir_id).expr_ty(expression).kind(),
            ty::Adt(definition, _) if definition.did() == tcx.parent(variant))
}

#[derive(Clone, PartialEq, Eq)]
struct Place {
    binding: HirId,
    fields: Vec<Symbol>,
}

impl Place {
    fn of(tcx: TyCtxt<'_>, expression: &Expr<'_>) -> Option<Self> {
        match expression.kind {
            ExprKind::Field(base, field) => {
                let mut place = Self::of(tcx, base)?;
                place.fields.push(field.name);
                Some(place)
            }
            ExprKind::AddrOf(_, _, base)
            | ExprKind::Unary(hir::UnOp::Deref, base)
            | ExprKind::DropTemps(base) => Self::of(tcx, base),
            _ => local_binding(tcx, expression).map(|binding| Self {
                binding,
                fields: Vec::new(),
            }),
        }
    }

    fn overlaps(&self, other: &Self) -> bool {
        self.binding == other.binding
            && (self.fields.starts_with(&other.fields) || other.fields.starts_with(&self.fields))
    }
}

struct Preparation {
    owner: Place,
    poll: HirId,
}

fn call_receiver<'a>(expression: &'a Expr<'a>) -> Option<&'a Expr<'a>> {
    match expression.kind {
        ExprKind::MethodCall(_, receiver, _, _) => Some(receiver),
        ExprKind::Call(_, arguments) => arguments.first(),
        _ => None,
    }
}

fn parent_step(tcx: TyCtxt<'_>, expression: &Expr<'_>, cursor: HirId) -> bool {
    let ExprKind::Match(condition, _, hir::MatchSource::TryDesugar(_)) = expression.kind else {
        return false;
    };
    let ExprKind::Call(_, [converted]) = condition.kind else {
        return false;
    };
    if resolved(tcx, condition) != tcx.lang_items().branch_fn() {
        return false;
    }
    let ExprKind::MethodCall(_, parent, _, _) = converted.kind else {
        return false;
    };
    let option = body_types(tcx, parent.hir_id).expr_ty(parent);
    if !matches!(option.kind(), ty::Adt(definition, _)
        if Some(definition.did()) == tcx.lang_items().option_type())
        || !resolved(tcx, converted).is_some_and(|id| {
            !id.is_local() && matches!(tcx.item_name(id).as_str(), "ok_or" | "ok_or_else")
        })
    {
        return false;
    }
    let ExprKind::MethodCall(_, receiver, [], _) = parent.kind else {
        return false;
    };
    local_binding(tcx, receiver) == Some(cursor)
        && resolved(tcx, parent).is_some_and(|id| tcx.def_path_str(id) == "std::path::Path::parent")
}

fn symbol(tcx: TyCtxt<'_>, id: DefId) -> String {
    if tcx.is_foreign_item(id)
        && let Some(name) = hir::find_attr!(tcx, id, LinkName { name, .. } => *name)
    {
        return name.to_string();
    }
    tcx.item_name(id).to_string()
}

fn clock_type(tcx: TyCtxt<'_>, expression: &Expr<'_>) -> bool {
    let ty = body_types(tcx, expression.hir_id)
        .expr_ty(expression)
        .peel_refs();
    if let ty::Adt(definition, _) = ty.kind() {
        let path = tcx.def_path_str(definition.did());
        path == "std::time::Instant"
            || path == "std::time::SystemTime"
            || path == "tokio::time::instant::Instant"
            || path == "tokio::time::Instant"
    } else {
        false
    }
}

fn duration_type(tcx: TyCtxt<'_>, expression: &Expr<'_>) -> bool {
    let ty = body_types(tcx, expression.hir_id)
        .expr_ty(expression)
        .peel_refs();
    if let ty::Adt(definition, _) = ty.kind() {
        matches!(
            tcx.def_path_str(definition.did()).as_str(),
            "core::time::Duration" | "std::time::Duration" | "tokio::time::Duration"
        )
    } else {
        false
    }
}

fn min_budget_type(tcx: TyCtxt<'_>, expression: &Expr<'_>) -> bool {
    let ty = body_types(tcx, expression.hir_id)
        .expr_ty(expression)
        .peel_refs();
    matches!(ty.kind(), ty::Uint(ty::UintTy::U64)) || duration_type(tcx, expression)
}

fn builtin_ref(tcx: TyCtxt<'_>, expression: &Expr<'_>) -> bool {
    matches!(
        body_types(tcx, expression.hir_id)
            .expr_ty(expression)
            .kind(),
        ty::Ref(..) | ty::RawPtr(..)
    )
}

fn no_custom_deref(tcx: TyCtxt<'_>, expression: &Expr<'_>) -> bool {
    body_types(tcx, expression.hir_id)
        .expr_adjustments(expression)
        .iter()
        .all(|adjustment| {
            !matches!(
                adjustment.kind,
                ty::adjustment::Adjust::Deref(ty::adjustment::DerefAdjustKind::Overloaded(_))
            )
        })
}

fn plain_place(tcx: TyCtxt<'_>, expression: &Expr<'_>) -> bool {
    match expression.kind {
        ExprKind::Field(base, _) | ExprKind::AddrOf(_, _, base) | ExprKind::DropTemps(base) => {
            no_custom_deref(tcx, base) && plain_place(tcx, base)
        }
        ExprKind::Unary(hir::UnOp::Deref, base) => {
            builtin_ref(tcx, base) && no_custom_deref(tcx, base) && plain_place(tcx, base)
        }
        _ => no_custom_deref(tcx, expression),
    }
}

fn observation(tcx: TyCtxt<'_>, id: DefId) -> bool {
    let name = tcx.item_name(id);
    let name = name.as_str();
    name == "try_recv"
        || name == "try_wait"
        || name == "try_lock"
        || (name == "exists" && tcx.def_path_str(id).starts_with("std::path::"))
        || (name == "load" && tcx.def_path_str(id).contains("::sync::atomic::"))
}

fn optional(tcx: TyCtxt<'_>, expression: &Expr<'_>) -> bool {
    let value = body_types(tcx, expression.hir_id)
        .expr_ty(expression)
        .peel_refs();
    if let ty::Adt(definition, _) = value.kind() {
        tcx.is_diagnostic_item(rustc_span::sym::Option, definition.did())
            || tcx.is_diagnostic_item(rustc_span::sym::Result, definition.did())
    } else {
        false
    }
}

// `is_some`/`is_none`/`is_ok`/`is_err` read which variant is present, not the
// clock value a variant may hold.
fn presence_test(tcx: TyCtxt<'_>, id: DefId, subject: &Expr<'_>) -> bool {
    matches!(
        tcx.item_name(id).as_str(),
        "is_some" | "is_none" | "is_ok" | "is_err"
    ) && optional(tcx, subject)
}

// A pattern that selects a variant and binds or ignores its payload, without
// comparing the payload to anything.
fn presence_pattern(pattern: &hir::Pat<'_>) -> bool {
    match pattern.kind {
        hir::PatKind::Wild | hir::PatKind::Binding(_, _, _, None) => true,
        hir::PatKind::Expr(hir::PatExpr {
            kind: hir::PatExprKind::Path(..),
            ..
        }) => true,
        hir::PatKind::TupleStruct(_, children, _) => children.iter().all(|child| {
            matches!(
                child.kind,
                hir::PatKind::Wild | hir::PatKind::Binding(_, _, _, None)
            )
        }),
        _ => false,
    }
}

fn returned_flags(tcx: TyCtxt<'_>, owner: DefId, flags: u8) -> u8 {
    // A helper's result is not proof that its caller supplied a request budget.
    let mut flags = flags & !REQUEST_BUDGET;
    if matches!(tcx.def_kind(owner), DefKind::Fn | DefKind::AssocFn) {
        let output = tcx
            .fn_sig(owner)
            .instantiate_identity()
            .skip_binder()
            .output();
        if !output.is_bool() && !output.is_integral() {
            flags &= !OBSERVATION;
        }
        if !carries_clock(tcx, output) {
            flags &= !(CLOCK | EXPIRY);
        }
    }
    flags
}

fn carries_clock(tcx: TyCtxt<'_>, value: ty::Ty<'_>) -> bool {
    let value = value.peel_refs();
    if value.is_bool() || value.is_integral() || value.is_floating_point() {
        return true;
    }
    if let ty::Adt(definition, arguments) = value.kind() {
        let path = tcx.def_path_str(definition.did());
        return path == "std::time::Instant"
            || path == "std::time::SystemTime"
            || path == "core::time::Duration"
            || ((path == "core::option::Option" || path == "core::result::Result")
                && carries_clock(tcx, arguments.type_at(0)));
    }
    false
}

#[derive(Default)]
struct Facts {
    expressions: HashMap<HirId, u8>,
    returns: HashMap<DefId, u8>,
    functions: HashMap<HirId, HashSet<DefId>>,
    values: HashMap<HirId, Vec<HirId>>,
    mutations: HashSet<HirId>,
    writing_calls: HashMap<HirId, u8>,
    // Flags written through a field projection of a binding, kept apart from
    // the binding so a measurement stored in one field does not taint reads
    // of its siblings.
    fields: HashMap<HirId, Vec<(Vec<Symbol>, u8)>>,
    changed: bool,
}

impl Facts {
    fn parameter_binding(
        &self,
        tcx: TyCtxt<'_>,
        binding: HirId,
        seen: &mut HashSet<HirId>,
    ) -> bool {
        if !seen.insert(binding) || self.mutations.contains(&binding) {
            return false;
        }
        let owner = binding.owner.def_id;
        if matches!(tcx.def_kind(owner), DefKind::Fn | DefKind::AssocFn)
            && tcx.hir_maybe_body_owned_by(owner).is_some_and(|body| {
                body.params.iter().any(|parameter| {
                    let mut supplied = false;
                    parameter
                        .pat
                        .each_binding(|_, id, _, _| supplied |= id == binding);
                    supplied
                })
            })
        {
            return true;
        }
        self.values.get(&binding).is_some_and(|values| {
            !values.is_empty()
                && values.iter().all(|value| {
                    matches!(tcx.hir_node(*value), hir::Node::Expr(value)
                        if self.parameter_origin(tcx, value, &mut seen.clone()))
                })
        })
    }

    fn parameter_origin(
        &self,
        tcx: TyCtxt<'_>,
        expression: &Expr<'_>,
        seen: &mut HashSet<HirId>,
    ) -> bool {
        local_binding(tcx, expression)
            .is_some_and(|binding| self.parameter_binding(tcx, binding, seen))
    }

    fn plain_clock(
        &self,
        tcx: TyCtxt<'_>,
        expression: &Expr<'_>,
        seen: &mut HashSet<HirId>,
    ) -> bool {
        if !clock_type(tcx, expression) || !seen.insert(expression.hir_id) {
            return false;
        }
        if matches!(expression.kind, ExprKind::Call(_, []))
            && resolved(tcx, expression).is_some_and(|id| {
                matches!(
                    tcx.def_path_str(id).as_str(),
                    "std::time::Instant::now"
                        | "tokio::time::Instant::now"
                        | "tokio::time::instant::Instant::now"
                )
            })
        {
            return true;
        }
        if let Some(place) = Place::of(tcx, expression) {
            if self.mutations.contains(&place.binding) || !plain_place(tcx, expression) {
                return false;
            }
            if self.parameter_binding(tcx, place.binding, &mut HashSet::new()) {
                return true;
            }
            return place.fields.is_empty()
                && self.values.get(&place.binding).is_some_and(|values| {
                    !values.is_empty()
                        && values.iter().all(|value| {
                            matches!(tcx.hir_node(*value), hir::Node::Expr(value)
                                if self.plain_clock(tcx, value, &mut seen.clone()))
                        })
                });
        }
        match expression.kind {
            ExprKind::Unary(hir::UnOp::Deref, value) => {
                builtin_ref(tcx, value) && self.plain_clock(tcx, value, seen)
            }
            ExprKind::Type(value, _)
            | ExprKind::DropTemps(value)
            | ExprKind::AddrOf(_, _, value)
            | ExprKind::Use(value, _) => self.plain_clock(tcx, value, seen),
            _ => false,
        }
    }

    fn all_values(
        &self,
        tcx: TyCtxt<'_>,
        expression: &Expr<'_>,
        predicate: &impl Fn(TyCtxt<'_>, &Expr<'_>) -> bool,
        seen: &mut HashSet<HirId>,
    ) -> bool {
        if !seen.insert(expression.hir_id) {
            return false;
        }
        if predicate(tcx, expression) {
            return true;
        }
        local_binding(tcx, expression)
            .and_then(|binding| self.values.get(&binding))
            .is_some_and(|values| {
                !values.is_empty()
                    && values.iter().all(|value| {
                        matches!(tcx.hir_node(*value), hir::Node::Expr(value)
                            if self.all_values(tcx, value, predicate, &mut seen.clone()))
                    })
            })
    }

    fn infinite_pipewire(&self, tcx: TyCtxt<'_>, expression: &Expr<'_>) -> bool {
        self.all_values(
            tcx,
            expression,
            &|tcx, value| pipewire_timeout(tcx, value, "Infinite"),
            &mut HashSet::new(),
        )
    }

    fn disabled_timeout(&self, tcx: TyCtxt<'_>, expression: &Expr<'_>) -> bool {
        self.all_values(tcx, expression, &is_none, &mut HashSet::new())
    }

    fn get(&self, expression: &Expr<'_>) -> u8 {
        self.expressions
            .get(&expression.hir_id)
            .copied()
            .unwrap_or(0)
    }

    fn merge(&mut self, id: HirId, flags: u8) {
        let current = self.expressions.entry(id).or_default();
        self.changed |= *current | flags != *current;
        *current |= flags;
    }

    fn merge_field(&mut self, binding: HirId, path: Vec<Symbol>, flags: u8) {
        let entries = self.fields.entry(binding).or_default();
        if let Some((_, current)) = entries.iter_mut().find(|(known, _)| *known == path) {
            self.changed |= *current | flags != *current;
            *current |= flags;
        } else if flags != 0 {
            entries.push((path, flags));
            self.changed = true;
        }
    }

    // Everything known about a binding, including every field written into it.
    fn whole(&self, binding: HirId) -> u8 {
        self.fields.get(&binding).into_iter().flatten().fold(
            self.expressions.get(&binding).copied().unwrap_or(0),
            |flags, (_, field)| flags | field,
        )
    }

    // What a read of `path` within a binding can observe: the binding's own
    // flags and those of fields that contain or lie within `path`.
    fn part(&self, binding: HirId, path: &[Symbol]) -> u8 {
        self.fields
            .get(&binding)
            .into_iter()
            .flatten()
            .filter(|(known, _)| known.starts_with(path) || path.starts_with(known))
            .fold(
                self.expressions.get(&binding).copied().unwrap_or(0),
                |flags, (_, field)| flags | field,
            )
    }

    // `let target = source;` where `source` is a place: the target sees what
    // covers the whole source place, and keeps the source's deeper field
    // entries as its own fields.
    fn copy_place(&mut self, target: HirId, source: &Place) {
        let entries = self
            .fields
            .get(&source.binding)
            .cloned()
            .unwrap_or_default();
        let mut flags = self.expressions.get(&source.binding).copied().unwrap_or(0);
        for (path, field) in entries {
            if source.fields.starts_with(&path) {
                flags |= field;
            } else if let Some(rest) = path.strip_prefix(source.fields.as_slice()) {
                self.merge_field(target, rest.to_vec(), field);
            }
        }
        self.merge(target, flags);
    }

    fn returned(&mut self, id: DefId, flags: u8) {
        let current = self.returns.entry(id).or_default();
        self.changed |= *current | flags != *current;
        *current |= flags;
    }

    fn assigned(&mut self, binding: HirId, value: HirId) {
        let values = self.values.entry(binding).or_default();
        if !values.contains(&value) {
            values.push(value);
            self.changed = true;
        }
    }
}

struct Propagate<'a, 'tcx> {
    tcx: TyCtxt<'tcx>,
    facts: &'a mut Facts,
    owner: DefId,
}

impl<'tcx> Visitor<'tcx> for Propagate<'_, 'tcx> {
    fn visit_local(&mut self, statement: &'tcx hir::LetStmt<'tcx>) {
        intravisit::walk_local(self, statement);
        if let Some(initializer) = statement.init {
            if let hir::PatKind::Binding(_, id, _, None) = statement.pat.kind
                && let Some(source) = Place::of(self.tcx, initializer)
            {
                self.facts.copy_place(id, &source);
                if clock_type(self.tcx, initializer) {
                    self.facts.merge(id, CLOCK);
                }
                self.copy_functions(id, initializer.hir_id);
                self.facts.assigned(id, initializer.hir_id);
                return;
            }
            let flags = self.facts.get(initializer);
            statement.pat.each_binding(|_, id, _, _| {
                self.facts.merge(id, flags);
                self.copy_functions(id, initializer.hir_id);
                self.facts.assigned(id, initializer.hir_id);
            });
        }
    }

    fn visit_expr(&mut self, expression: &'tcx Expr<'tcx>) {
        intravisit::walk_expr(self, expression);
        let mut flags = if clock_type(self.tcx, expression) {
            CLOCK
        } else {
            0
        };
        match expression.kind {
            ExprKind::Path(ref path) => {
                let typeck = body_types(self.tcx, expression.hir_id);
                match typeck.qpath_res(path, expression.hir_id) {
                    Res::Local(id) => {
                        flags |= self.facts.whole(id);
                        self.copy_functions(expression.hir_id, id);
                    }
                    Res::Def(DefKind::Fn | DefKind::AssocFn, id) => {
                        self.facts.changed |= self
                            .facts
                            .functions
                            .entry(expression.hir_id)
                            .or_default()
                            .insert(id);
                    }
                    Res::Def(
                        DefKind::Const { .. } | DefKind::AssocConst { .. } | DefKind::Static { .. },
                        id,
                    ) => {
                        flags |= self.facts.returns.get(&id).copied().unwrap_or(0);
                    }
                    _ => {}
                }
            }
            ExprKind::Call(callee, arguments) => {
                flags |= self.facts.get(callee) & !REQUEST_BUDGET;
                for argument in arguments {
                    flags |= self.facts.get(argument) & !REQUEST_BUDGET;
                }
                for id in self
                    .facts
                    .functions
                    .get(&callee.hir_id)
                    .into_iter()
                    .flatten()
                {
                    flags |= self.facts.returns.get(id).copied().unwrap_or(0);
                    if observation(self.tcx, *id) {
                        flags |= OBSERVATION;
                    }
                }
            }
            ExprKind::MethodCall(segment, receiver, arguments, _) => {
                let receiver_flags = self.facts.get(receiver);
                flags |= receiver_flags & !REQUEST_BUDGET;
                for argument in arguments {
                    flags |= self.facts.get(argument) & !REQUEST_BUDGET;
                }
                let standard_method = no_custom_deref(self.tcx, receiver)
                    && resolved(self.tcx, expression).is_some_and(|id| {
                        !id.is_local()
                            && matches!(
                                self.tcx.crate_name(id.krate).as_str(),
                                "core" | "std" | "serde_json"
                            )
                    });
                if standard_method
                    && matches!(
                        segment.ident.as_str(),
                        "as_u64"
                            | "unwrap"
                            | "expect"
                            | "unwrap_or"
                            | "unwrap_or_default"
                            | "map_err"
                    )
                {
                    flags |= receiver_flags & REQUEST_BUDGET;
                } else if standard_method
                    && segment.ident.as_str() == "min"
                    && let [other] = arguments
                    && min_budget_type(self.tcx, receiver)
                    && min_budget_type(self.tcx, other)
                {
                    flags |= (receiver_flags | self.facts.get(other)) & REQUEST_BUDGET;
                }
            }
            ExprKind::Index(left, right, _) => {
                flags |= self.facts.get(left) | (self.facts.get(right) & !REQUEST_BUDGET);
            }
            ExprKind::Binary(op, left, right) => {
                let left_flags = self.facts.get(left);
                let right_flags = self.facts.get(right);
                flags |= (left_flags | right_flags) & !REQUEST_BUDGET;
                if op.node == hir::BinOpKind::Add
                    && ((self.facts.plain_clock(self.tcx, left, &mut HashSet::new())
                        && left_flags & REQUEST_BUDGET == 0
                        && duration_type(self.tcx, right)
                        && right_flags & REQUEST_BUDGET != 0)
                        || (self.facts.plain_clock(self.tcx, right, &mut HashSet::new())
                            && right_flags & REQUEST_BUDGET == 0
                            && duration_type(self.tcx, left)
                            && left_flags & REQUEST_BUDGET != 0))
                {
                    flags |= REQUEST_BUDGET;
                }
            }
            ExprKind::Field(value, _) => {
                let field_flags = match Place::of(self.tcx, expression) {
                    Some(place) => self.facts.part(place.binding, &place.fields),
                    None => self.facts.get(value),
                };
                flags |= if plain_place(self.tcx, expression) {
                    field_flags
                } else {
                    field_flags & !REQUEST_BUDGET
                };
                self.copy_functions(expression.hir_id, value.hir_id);
            }
            ExprKind::Unary(hir::UnOp::Deref, value) => {
                flags |= if builtin_ref(self.tcx, value) {
                    self.facts.get(value)
                } else {
                    self.facts.get(value) & !REQUEST_BUDGET
                };
                self.copy_functions(expression.hir_id, value.hir_id);
            }
            ExprKind::Type(value, _)
            | ExprKind::DropTemps(value)
            | ExprKind::AddrOf(_, _, value)
            | ExprKind::Use(value, _) => {
                flags |= self.facts.get(value);
                self.copy_functions(expression.hir_id, value.hir_id);
            }
            ExprKind::Unary(hir::UnOp::Not | hir::UnOp::Neg, value) | ExprKind::Cast(value, _) => {
                flags |= self.facts.get(value) & !REQUEST_BUDGET;
                self.copy_functions(expression.hir_id, value.hir_id);
            }
            ExprKind::Block(block, _) => {
                if let Some(tail) = block.expr {
                    flags |= self.facts.get(tail);
                }
            }
            ExprKind::If(condition, then, otherwise) => {
                let then_flags = self.facts.get(then);
                flags |= (self.facts.get(condition) | then_flags) & !REQUEST_BUDGET;
                if let Some(otherwise) = otherwise {
                    let otherwise_flags = self.facts.get(otherwise);
                    flags |= otherwise_flags & !REQUEST_BUDGET;
                    let then_never = body_types(self.tcx, then.hir_id).expr_ty(then).is_never();
                    let otherwise_never = body_types(self.tcx, otherwise.hir_id)
                        .expr_ty(otherwise)
                        .is_never();
                    let mut branch_budget = REQUEST_BUDGET;
                    if !then_never {
                        branch_budget &= then_flags & REQUEST_BUDGET;
                    }
                    if !otherwise_never {
                        branch_budget &= otherwise_flags & REQUEST_BUDGET;
                    }
                    if !then_never || !otherwise_never {
                        flags |= branch_budget;
                    }
                }
            }
            ExprKind::Match(value, arms, _) => {
                let value_flags = self.facts.get(value);
                flags |= value_flags & !REQUEST_BUDGET;
                let mut arm_budget = REQUEST_BUDGET;
                let mut value_arms = false;
                for arm in arms {
                    arm.pat.each_binding(|_, id, _, _| {
                        self.facts.merge(id, value_flags);
                        self.facts.assigned(id, value.hir_id);
                    });
                    let body_flags = self.facts.get(arm.body);
                    flags |= body_flags & !REQUEST_BUDGET;
                    if !body_types(self.tcx, arm.body.hir_id)
                        .expr_ty(arm.body)
                        .is_never()
                    {
                        value_arms = true;
                        arm_budget &= body_flags & REQUEST_BUDGET;
                    }
                }
                if value_arms {
                    flags |= arm_budget;
                }
            }
            ExprKind::Let(value) => {
                flags |= self.facts.get(value.init);
                value.pat.each_binding(|_, id, _, _| {
                    self.facts.merge(id, flags);
                    self.facts.assigned(id, value.init.hir_id);
                });
            }
            ExprKind::Assign(left, right, _) => {
                self.write_place(
                    left,
                    right.hir_id,
                    self.facts.get(right),
                    &mut HashSet::new(),
                );
            }
            ExprKind::AssignOp(_, left, right) => {
                let written = (self.facts.get(left) | self.facts.get(right)) & !REQUEST_BUDGET;
                self.write_place(left, expression.hir_id, written, &mut HashSet::new());
            }
            ExprKind::Ret(Some(value)) => self.facts.returned(
                self.owner,
                returned_flags(self.tcx, self.owner, self.facts.get(value)),
            ),
            _ => {}
        }
        if matches!(
            expression.kind,
            ExprKind::Call(..) | ExprKind::MethodCall(..)
        ) && let Some(id) = resolved(self.tcx, expression)
        {
            if self.tcx.crate_name(id.krate).as_str() == "serde_json"
                && self.tcx.item_name(id).as_str() == "from_value"
                && let ExprKind::Call(_, [input]) = expression.kind
                && self
                    .facts
                    .parameter_origin(self.tcx, input, &mut HashSet::new())
            {
                flags |= REQUEST_BUDGET;
            }
            if (Some(id) == self.tcx.lang_items().branch_fn()
                || matches!(
                    self.tcx.def_path_str(id).as_str(),
                    "core::time::Duration::from_millis" | "std::time::Duration::from_millis"
                ))
                && let ExprKind::Call(_, [input]) = expression.kind
            {
                flags |= self.facts.get(input) & REQUEST_BUDGET;
            }
            if matches!(
                self.tcx.def_path_str(id).as_str(),
                "core::cmp::Ord::min" | "std::cmp::Ord::min"
            ) && let ExprKind::Call(_, [left, right]) = expression.kind
                && min_budget_type(self.tcx, left)
                && min_budget_type(self.tcx, right)
            {
                flags |= (self.facts.get(left) | self.facts.get(right)) & REQUEST_BUDGET;
            }
            flags |= self.facts.returns.get(&id).copied().unwrap_or(0);
            if observation(self.tcx, id) {
                flags |= OBSERVATION;
            }
            if pipewire_dispatch(self.tcx, id)
                && let ExprKind::Call(_, arguments) | ExprKind::MethodCall(_, _, arguments, _) =
                    expression.kind
                && arguments.last().is_some_and(|value| {
                    self.facts.all_values(
                        self.tcx,
                        value,
                        &|tcx, value| pipewire_timeout(tcx, value, "None"),
                        &mut HashSet::new(),
                    )
                })
            {
                flags |= OBSERVATION;
            }
            if self.tcx.def_path_str(id) == "std::time::Instant::checked_duration_since" {
                flags |= CLOCK | EXPIRY;
            }
            if let ExprKind::MethodCall(_, subject, [], _) | ExprKind::Call(_, [subject]) =
                expression.kind
                && presence_test(self.tcx, id, subject)
                && self.facts.get(subject) & EXPIRY == 0
            {
                flags &= !CLOCK;
            }
        }
        flags |= self.call_writes(expression);
        self.facts.merge(expression.hir_id, flags);
    }
}

impl Propagate<'_, '_> {
    fn write_place(
        &mut self,
        target: &Expr<'_>,
        value: HirId,
        flags: u8,
        seen: &mut HashSet<HirId>,
    ) {
        if let Some(place) = Place::of(self.tcx, target) {
            self.write_into(place, value, flags, seen);
        }
    }

    fn write_into(&mut self, place: Place, value: HirId, flags: u8, seen: &mut HashSet<HirId>) {
        if !seen.insert(place.binding) {
            return;
        }
        let aliases = self
            .facts
            .values
            .get(&place.binding)
            .cloned()
            .unwrap_or_default();
        if place.fields.is_empty() {
            self.facts.merge(place.binding, flags);
        } else {
            self.facts
                .merge_field(place.binding, place.fields.clone(), flags);
        }
        self.facts.assigned(place.binding, value);
        self.copy_functions(place.binding, value);
        self.facts.changed |= self.facts.mutations.insert(place.binding);
        for alias in aliases {
            if let hir::Node::Expr(alias) = self.tcx.hir_node(alias)
                && matches!(
                    body_types(self.tcx, alias.hir_id).expr_ty(alias).kind(),
                    ty::Ref(_, _, hir::Mutability::Mut) | ty::RawPtr(_, hir::Mutability::Mut)
                )
                && let Some(mut target) = Place::of(self.tcx, alias)
            {
                // A write through `alias.path` lands at the same path of the
                // place the reference points into.
                target.fields.extend(place.fields.iter().copied());
                self.write_into(target, value, flags, seen);
            }
        }
    }

    fn call_writes(&mut self, expression: &Expr<'_>) -> u8 {
        let (arguments, mut functions) = match expression.kind {
            ExprKind::Call(callee, arguments) => (
                arguments.iter().collect::<Vec<_>>(),
                self.facts
                    .functions
                    .get(&callee.hir_id)
                    .cloned()
                    .unwrap_or_default(),
            ),
            ExprKind::MethodCall(_, receiver, arguments, _) => (
                std::iter::once(receiver).chain(arguments).collect(),
                HashSet::new(),
            ),
            _ => return 0,
        };
        if let Some(function) = resolved(self.tcx, expression) {
            functions.insert(function);
        }
        let mut writes = 0;
        for function in functions {
            let body = function
                .as_local()
                .and_then(|owner| self.tcx.hir_maybe_body_owned_by(owner));
            for (index, argument) in arguments.iter().enumerate() {
                let argument_type =
                    body_types(self.tcx, argument.hir_id).expr_ty_adjusted(argument);
                if !matches!(
                    argument_type.kind(),
                    ty::Ref(_, _, hir::Mutability::Mut) | ty::RawPtr(_, hir::Mutability::Mut)
                ) {
                    continue;
                }
                let (mutated, flags) = if let Some(body) = body {
                    let mut mutated = false;
                    let mut flags = 0;
                    if let Some(parameter) = body.params.get(index) {
                        parameter.pat.each_binding(|_, binding, _, _| {
                            mutated |= self.facts.mutations.contains(&binding);
                            flags |= self.facts.whole(binding);
                        });
                    }
                    (mutated, flags)
                } else {
                    (true, 0)
                };
                if mutated {
                    self.write_place(argument, expression.hir_id, flags, &mut HashSet::new());
                    let current = self
                        .facts
                        .writing_calls
                        .entry(expression.hir_id)
                        .or_default();
                    self.facts.changed |= *current | flags != *current;
                    *current |= flags;
                    writes |= flags;
                }
            }
        }
        writes
    }

    fn copy_functions(&mut self, target: HirId, source: HirId) {
        let functions = self
            .facts
            .functions
            .get(&source)
            .cloned()
            .unwrap_or_default();
        let target = self.facts.functions.entry(target).or_default();
        for function in functions {
            self.facts.changed |= target.insert(function);
        }
    }
}

struct Declarations<'tcx> {
    tcx: TyCtxt<'tcx>,
}

impl Declarations<'_> {
    fn attributes(&self, id: HirId) {
        for attribute in self.tcx.hir_attrs(id) {
            if let hir::Attribute::Parsed(AttributeKind::Deprecated { deprecation, span }) =
                attribute
                && let Some(note) = deprecation.note
            {
                let note = note.as_str().to_ascii_lowercase();
                if note.contains("renamed")
                    || note.contains("compatib")
                    || note.contains("superseded")
                    || note.contains("moved to")
                {
                    error(
                        self.tcx,
                        *span,
                        "compatibility_surface",
                        "remove the superseded local API and update its callers",
                    );
                }
            }
        }
    }
}

impl<'tcx> Visitor<'tcx> for Declarations<'tcx> {
    fn visit_item(&mut self, item: &'tcx hir::Item<'tcx>) {
        self.attributes(item.hir_id());
        if let hir::ItemKind::Const(ident, ..) | hir::ItemKind::Static(_, ident, ..) = item.kind
            && generation_field(ident.as_str())
        {
            error(
                self.tcx,
                item.span,
                "schema_generation",
                "an internal contract must not declare a schema/format generation constant",
            );
        }
        if let hir::ItemKind::Enum(ident, _, definition) = item.kind
            && contract(ident.as_str())
        {
            for variant in definition.variants {
                if numbered_generation(variant.ident.as_str()) {
                    error(
                        self.tcx,
                        variant.span,
                        "format_generation",
                        "internal formats have one current representation, not numbered variants",
                    );
                }
            }
        }
        if let hir::ItemKind::Fn { ident, .. } = item.kind {
            let name = normalized(ident.as_str());
            if name.contains("migrat") && contract(ident.as_str()) {
                error(
                    self.tcx,
                    item.span,
                    "compatibility_surface",
                    "remove internal schema/format migration entry points",
                );
            }
        }
        intravisit::walk_item(self, item);
    }

    fn visit_impl_item(&mut self, item: &'tcx hir::ImplItem<'tcx>) {
        self.attributes(item.hir_id());
        intravisit::walk_impl_item(self, item);
    }

    fn visit_trait_item(&mut self, item: &'tcx hir::TraitItem<'tcx>) {
        self.attributes(item.hir_id());
        intravisit::walk_trait_item(self, item);
    }

    fn visit_field_def(&mut self, field: &'tcx hir::FieldDef<'tcx>) {
        let parent = self.tcx.parent(field.def_id.to_def_id());
        let name = normalized(field.ident.as_str());
        if generation_field(&name)
            || ((name == "version" || name == "generation")
                && contract(&self.tcx.item_name(parent).to_string()))
        {
            error(
                self.tcx,
                field.span,
                "schema_generation",
                "remove internal schema/format generation fields; update every producer and consumer",
            );
        }
        intravisit::walk_field_def(self, field);
    }
}

struct Exits(bool);

impl<'tcx> Visitor<'tcx> for Exits {
    fn visit_expr(&mut self, expression: &'tcx Expr<'tcx>) {
        if matches!(
            expression.kind,
            ExprKind::Ret(..) | ExprKind::Break(..) | ExprKind::Continue(..)
        ) {
            self.0 = true;
        }
        intravisit::walk_expr(self, expression);
    }
}

struct CheckBody<'a, 'tcx> {
    tcx: TyCtxt<'tcx>,
    facts: &'a Facts,
    loops: usize,
    polling: Vec<(HirId, bool)>,
    effects: &'a Effects,
    api_labels: &'a HashSet<(Span, String)>,
}

impl CheckBody<'_, '_> {
    fn external_identifier(&self, expression: &Expr<'_>, value: &str) -> bool {
        let expansion = expression.span.ctxt().outer_expn_data();
        expansion.macro_def_id.is_some_and(|id| {
            self.tcx.crate_name(id.krate).as_str() == "core"
                && self.tcx.item_name(id).as_str() == "stringify"
        }) && self
            .api_labels
            .contains(&(expression.span.source_callsite(), value.to_owned()))
    }

    fn key_context(&self, expression: &Expr<'_>) -> bool {
        let mut child = expression.hir_id;
        for (id, node) in self.tcx.hir_parent_iter(child) {
            if let hir::Node::Expr(parent) = node {
                match parent.kind {
                    ExprKind::Tup(values)
                        if values.first().is_some_and(|value| value.hir_id == child) =>
                    {
                        return true;
                    }
                    ExprKind::MethodCall(_, _, arguments, _) | ExprKind::Call(_, arguments)
                        if resolved(self.tcx, parent)
                            .is_some_and(|id| self.tcx.item_name(id).as_str() == "insert")
                            && arguments.first().is_some_and(|value| value.hir_id == child) =>
                    {
                        return true;
                    }
                    _ => {}
                }
            }
            child = id;
        }
        false
    }

    fn external_api_value(
        tcx: TyCtxt<'_>,
        expression: &Expr<'_>,
        parameters: &HashSet<HirId>,
        seen: &mut HashSet<DefId>,
    ) -> bool {
        match expression.kind {
            ExprKind::Path(ref path) => {
                match body_types(tcx, expression.hir_id).qpath_res(path, expression.hir_id) {
                    Res::Local(id) => parameters.contains(&id),
                    Res::Def(DefKind::Const { .. } | DefKind::AssocConst { .. }, id) => {
                        if !id.is_local() {
                            return body_types(tcx, expression.hir_id)
                                .expr_ty(expression)
                                .is_integral()
                                && matches!(tcx.def_kind(tcx.parent(id)), DefKind::Mod)
                                && tcx.module_children(tcx.parent(id)).iter().any(|child| {
                                    child
                                        .res
                                        .opt_def_id()
                                        .is_some_and(|id| tcx.is_foreign_item(id))
                                });
                        }
                        seen.insert(id)
                            && tcx
                                .hir_maybe_body_owned_by(id.expect_local())
                                .is_some_and(|body| {
                                    Self::external_api_value(tcx, body.value, parameters, seen)
                                })
                    }
                    _ => false,
                }
            }
            ExprKind::Block(block, _) if block.stmts.is_empty() => block
                .expr
                .is_some_and(|value| Self::external_api_value(tcx, value, parameters, seen)),
            ExprKind::Call(_, [argument]) => {
                let Some(id) = resolved(tcx, expression) else {
                    return false;
                };
                if !Self::external_api_value(tcx, argument, parameters, &mut seen.clone()) {
                    return false;
                }
                if matches!(tcx.def_kind(id), DefKind::Ctor(..)) {
                    return true;
                }
                if let Some(local) = id.as_local()
                    && seen.insert(id)
                    && let Some(body) = tcx.hir_maybe_body_owned_by(local)
                    && let [parameter] = body.params
                {
                    let mut parameters = HashSet::new();
                    parameter.pat.each_binding(|_, id, _, _| {
                        parameters.insert(id);
                    });
                    return Self::external_api_value(tcx, body.value, &parameters, seen);
                }
                false
            }
            _ => false,
        }
    }

    fn external_api_labels(tcx: TyCtxt<'_>) -> HashSet<(Span, String)> {
        let mut labels = HashSet::new();
        for owner in tcx.hir_body_owners() {
            if matches!(tcx.def_kind(owner), DefKind::AssocConst { .. })
                && tcx.hir_body_owned_by(owner).value.span.from_expansion()
                && Self::external_api_value(
                    tcx,
                    tcx.hir_body_owned_by(owner).value,
                    &HashSet::new(),
                    &mut HashSet::new(),
                )
            {
                labels.insert((
                    tcx.hir_body_owned_by(owner).value.span.source_callsite(),
                    tcx.item_name(owner.to_def_id()).to_string(),
                ));
            }
        }
        labels
    }

    fn condition(&self, condition: &Expr<'_>, exits: bool) {
        let flags = self.facts.get(condition);
        if flags & CLOCK != 0 && (self.loops != 0 || exits) {
            error(
                self.tcx,
                condition.span,
                "timing_control",
                "clock-derived data controls waiting or early completion; use completion/cancellation events",
            );
        }
        if flags & OBSERVATION != 0
            && self.polling.iter().any(|(id, repeats)| {
                *repeats
                    && !self.effects.finite.get(id).is_some_and(|bindings| {
                        self.element_observations(condition, bindings, *id, &mut HashSet::new())
                            == (true, true)
                    })
            })
        {
            error(
                self.tcx,
                condition.span,
                "readiness_poll",
                "a loop repeatedly observes readiness; wait for the event instead",
            );
        }
    }

    fn element_observations(
        &self,
        expression: &Expr<'_>,
        bindings: &HashSet<HirId>,
        iteration: HirId,
        seen: &mut HashSet<HirId>,
    ) -> (bool, bool) {
        if !seen.insert(expression.hir_id) {
            return (false, true);
        }
        let mut found = false;
        let mut bound = true;
        if matches!(
            expression.kind,
            ExprKind::Call(..) | ExprKind::MethodCall(..)
        ) && (resolved(self.tcx, expression).is_some_and(|id| {
            observation(self.tcx, id)
                || self.facts.returns.get(&id).copied().unwrap_or(0) & OBSERVATION != 0
        }) || self
            .facts
            .writing_calls
            .get(&expression.hir_id)
            .is_some_and(|flags| flags & OBSERVATION != 0)
            || (self.facts.get(expression) & OBSERVATION != 0
                && resolved(self.tcx, expression)
                    .is_some_and(|id| pipewire_dispatch(self.tcx, id))))
        {
            found = true;
            bound = !self
                .tcx
                .hir_parent_iter(expression.hir_id)
                .any(|(parent, _)| parent == iteration)
                || self.per_element(expression, bindings, &mut HashSet::new());
        }
        if let ExprKind::Path(ref path) = expression.kind
            && let Res::Local(id) =
                body_types(self.tcx, expression.hir_id).qpath_res(path, expression.hir_id)
            && let Some(values) = self.facts.values.get(&id)
        {
            for value in values {
                if let hir::Node::Expr(value) = self.tcx.hir_node(*value) {
                    let next = self.element_observations(value, bindings, iteration, seen);
                    found |= next.0;
                    bound &= next.1;
                }
            }
        }
        for child in expression_children(expression) {
            let next = self.element_observations(child, bindings, iteration, seen);
            found |= next.0;
            bound &= next.1;
        }
        (found, bound)
    }

    fn per_element(
        &self,
        expression: &Expr<'_>,
        bindings: &HashSet<HirId>,
        seen: &mut HashSet<HirId>,
    ) -> bool {
        if !seen.insert(expression.hir_id) {
            return false;
        }
        if let ExprKind::Path(ref path) = expression.kind
            && let Res::Local(id) =
                body_types(self.tcx, expression.hir_id).qpath_res(path, expression.hir_id)
        {
            return bindings.contains(&id)
                || self.facts.values.get(&id).is_some_and(|values| {
                    values.iter().any(|id| {
                        matches!(self.tcx.hir_node(*id), hir::Node::Expr(value)
                    if self.per_element(value, bindings, seen))
                    })
                });
        }
        expression_children(expression)
            .iter()
            .any(|child| self.per_element(child, bindings, seen))
    }

    fn item_reference(&self, id: DefId, span: Span, request_deadline: bool) {
        if id.is_local() && !self.tcx.is_foreign_item(id) {
            return;
        }
        let name = normalized(&symbol(self.tcx, id));
        let path = self.tcx.def_path_str(id);
        if request_deadline
            && name == "sleepuntil"
            && self.tcx.crate_name(id.krate).as_str() == "tokio"
        {
            return;
        }
        let timing = name == "sleep"
            || name == "sleepex"
            || name == "nanosleep"
            || name == "usleep"
            || name == "clocknanosleep"
            || name == "parktimeout"
            || name == "sleepuntil"
            || name.starts_with("recvtimeout")
            || name.starts_with("recvdeadline")
            || name.starts_with("waittimeout")
            || name.starts_with("waitdeadline")
            || (name.contains("timer")
                && (name.starts_with("set")
                    || name.starts_with("create")
                    || name.starts_with("arm")))
            || (path.contains("::time::")
                && (name.starts_with("interval")
                    || name.starts_with("timeout")
                    || name.starts_with("sleep")
                    || name.starts_with("reset")));
        if timing {
            error(
                self.tcx,
                span,
                "timed_wait",
                &format!("resolved timing capability `{path}` cannot authorize an internal wait"),
            );
        }
    }

    fn wait_arguments(&self, expression: &Expr<'_>, id: DefId, arguments: &[Expr<'_>]) {
        if id.is_local() && !self.tcx.is_foreign_item(id) {
            return;
        }
        let name = normalized(&symbol(self.tcx, id));
        if pipewire_dispatch(self.tcx, id) {
            if !arguments.last().is_some_and(|value| {
                self.facts.infinite_pipewire(self.tcx, value)
                    || (self.loops == 0
                        && self.facts.all_values(
                            self.tcx,
                            value,
                            &|tcx, value| pipewire_timeout(tcx, value, "None"),
                            &mut HashSet::new(),
                        ))
            }) {
                error(
                    self.tcx,
                    expression.span,
                    "native_timed_wait",
                    "PipeWire dispatch requires the native Timeout::Infinite variant",
                );
            }
        } else if native_method(
            self.tcx,
            id,
            "libpulse_binding::mainloop::standard::Mainloop",
            "prepare",
        ) {
            if !arguments
                .last()
                .is_some_and(|value| self.facts.disabled_timeout(self.tcx, value))
            {
                error(
                    self.tcx,
                    expression.span,
                    "native_timed_wait",
                    "Pulse preparation requires an unbounded native event wait",
                );
            }
        } else if native_method(
            self.tcx,
            id,
            "libpulse_binding::mainloop::standard::Mainloop",
            "poll",
        ) {
            if !self.effects.prepared.contains(&expression.hir_id) {
                error(
                    self.tcx,
                    expression.span,
                    "native_timed_wait",
                    "Pulse polling requires checked unbounded preparation on the same native owner",
                );
            }
        } else if name == "setreadtimeout" || name == "setwritetimeout" {
            if !arguments
                .last()
                .is_some_and(|argument| self.facts.disabled_timeout(self.tcx, argument))
            {
                error(
                    self.tcx,
                    expression.span,
                    "timed_wait",
                    "a socket timeout must be disabled, not set to an internal budget",
                );
            }
        } else if name.starts_with("waitfor") && name.contains("object") {
            let index = arguments
                .len()
                .checked_sub(if name.ends_with("ex") { 2 } else { 1 });
            if let Some(argument) = index.and_then(|index| arguments.get(index)) {
                let value = scalar(self.tcx, argument);
                if value != Some(u32::MAX as u128) && !(value == Some(0) && self.loops == 0) {
                    error(
                        self.tcx,
                        argument.span,
                        "native_timed_wait",
                        "native object wait requires INFINITE; a zero-budget snapshot must not poll in a loop",
                    );
                }
            }
        } else if self.tcx.is_foreign_item(id)
            && name == "poll"
            && let Some(argument) = arguments.last()
        {
            let value = scalar(self.tcx, argument);
            if value != Some(u128::MAX)
                && value != Some(u32::MAX as u128)
                && !(value == Some(0) && self.loops == 0)
            {
                error(
                    self.tcx,
                    argument.span,
                    "native_timed_wait",
                    "poll requires the ABI's infinite wait; a zero-budget snapshot must not poll in a loop",
                );
            }
        }
    }
}

fn expression_children<'a>(expression: &'a Expr<'a>) -> Vec<&'a Expr<'a>> {
    struct Children<'a> {
        expressions: Vec<&'a Expr<'a>>,
    }
    impl<'a> Visitor<'a> for Children<'a> {
        fn visit_expr(&mut self, expression: &'a Expr<'a>) {
            self.expressions.push(expression);
        }
    }
    let mut children = Children {
        expressions: Vec::new(),
    };
    intravisit::walk_expr(&mut children, expression);
    children.expressions
}

#[derive(Default)]
struct Progress {
    // Bits retain unproved (1) and proved (2) paths for the current progress goal.
    fallthrough: u8,
    breaks: HashMap<HirId, u8>,
    continues: HashMap<HirId, u8>,
    returns: u8,
    productive_returns: u8,
    target_inputs: u8,
}

impl Progress {
    fn starting(input: u8) -> Self {
        Self {
            fallthrough: input,
            ..Self::default()
        }
    }

    fn exits(&mut self, other: Self) {
        for (id, paths) in other.breaks {
            *self.breaks.entry(id).or_default() |= paths;
        }
        for (id, paths) in other.continues {
            *self.continues.entry(id).or_default() |= paths;
        }
        self.returns |= other.returns;
        self.productive_returns |= other.productive_returns;
        self.target_inputs |= other.target_inputs;
    }

    fn sequence(&mut self, other: Self) {
        self.fallthrough = other.fallthrough;
        self.exits(other);
    }
}

#[derive(Default)]
struct Effects {
    always: HashSet<DefId>,
    productive: HashSet<DefId>,
    finite: HashMap<HirId, HashSet<HirId>>,
    prepared: HashSet<HirId>,
}

struct Flow<'a, 'tcx> {
    tcx: TyCtxt<'tcx>,
    facts: &'a Facts,
    effects: &'a Effects,
    iteration: Option<HirId>,
    descent: Option<HirId>,
    preparation: Option<&'a Preparation>,
}

impl<'tcx> Flow<'_, 'tcx> {
    fn event_wait(&self, expression: &Expr<'_>) -> bool {
        if self.descent.is_some() || self.preparation.is_some() {
            return false;
        }
        let Some(id) = resolved(self.tcx, expression) else {
            return false;
        };
        if self.effects.always.contains(&id) {
            return true;
        }
        let path = self.tcx.def_path_str(id);
        let name = self.tcx.item_name(id);
        if path == "std::thread::park"
            || (path.starts_with("std::sync::")
                && (name.as_str() == "recv" || name.as_str() == "wait"))
        {
            return true;
        }
        if id.is_local() && !self.tcx.is_foreign_item(id) {
            return false;
        }
        let name = normalized(&symbol(self.tcx, id));
        let arguments = match expression.kind {
            ExprKind::Call(_, arguments) | ExprKind::MethodCall(_, _, arguments, _) => arguments,
            _ => return false,
        };
        if pipewire_dispatch(self.tcx, id) {
            return arguments
                .last()
                .is_some_and(|value| self.facts.infinite_pipewire(self.tcx, value));
        }
        if name.starts_with("waitfor") && name.contains("object") {
            return arguments
                .len()
                .checked_sub(if name.ends_with("ex") { 2 } else { 1 })
                .and_then(|index| arguments.get(index))
                .and_then(|value| scalar(self.tcx, value))
                == Some(u32::MAX as u128);
        }
        self.tcx.is_foreign_item(id)
            && name == "poll"
            && arguments
                .last()
                .and_then(|value| scalar(self.tcx, value))
                .is_some_and(|value| value == u128::MAX || value == u32::MAX as u128)
    }

    fn primitive_consumer(&self, expression: &Expr<'_>, id: DefId) -> bool {
        if id.is_local() {
            return false;
        }
        if self.accepted_connection(expression, id) {
            return true;
        }
        if native_method(self.tcx, id, "libpulse_binding::stream::Stream", "discard")
            || self.effects.prepared.contains(&expression.hir_id)
        {
            return true;
        }
        let path = self.tcx.def_path_str(id);
        let name = self.tcx.item_name(id);
        if (path.starts_with("std::sync::mpsc::") && name.as_str() == "try_recv")
            || (path.starts_with("rtrb::Consumer") && name.as_str() == "pop")
        {
            return true;
        }
        path.starts_with("windows::Win32::Media::Audio::IAudioCaptureClient::")
            && name.as_str() == "ReleaseBuffer"
            && matches!(expression.kind, ExprKind::MethodCall(_, _, [amount], _)
                if scalar(self.tcx, amount) != Some(0))
    }

    fn accepted_connection(&self, expression: &Expr<'_>, id: DefId) -> bool {
        if Some(id) != self.tcx.lang_items().next_fn() {
            return false;
        }
        let Some(receiver) = call_receiver(expression) else {
            return false;
        };
        let receiver = body_types(self.tcx, receiver.hir_id)
            .expr_ty(receiver)
            .peel_refs();
        let ty::Adt(definition, _) = receiver.kind() else {
            return false;
        };
        self.tcx
            .get_diagnostic_item(Symbol::intern("IoRead"))
            .is_some_and(|read| definition.did().krate == read.krate)
            && matches!(
                self.tcx.def_path_str(definition.did()).as_str(),
                "std::net::Incoming" | "std::os::unix::net::Incoming"
            )
    }

    fn productive(&self, expression: &Expr<'_>, seen: &mut HashSet<HirId>) -> bool {
        if self.descent.is_some() {
            return false;
        }
        if !seen.insert(expression.hir_id) {
            return false;
        }
        if let Some(preparation) = self.preparation
            && let Some(id) = resolved(self.tcx, expression)
            && native_method(
                self.tcx,
                id,
                "libpulse_binding::mainloop::standard::Mainloop",
                "prepare",
            )
        {
            let arguments = match expression.kind {
                ExprKind::Call(_, arguments) | ExprKind::MethodCall(_, _, arguments, _) => {
                    arguments
                }
                _ => return false,
            };
            return call_receiver(expression)
                .and_then(|receiver| Place::of(self.tcx, receiver))
                .is_some_and(|owner| owner == preparation.owner)
                && arguments
                    .last()
                    .is_some_and(|value| self.facts.disabled_timeout(self.tcx, value));
        }
        if let Some(id) = resolved(self.tcx, expression)
            && self.preparation.is_none()
            && (self.effects.productive.contains(&id) || self.primitive_consumer(expression, id))
        {
            return true;
        }
        match expression.kind {
            ExprKind::Path(ref path) => {
                if let Res::Local(id) = body_types(self.tcx, expression.hir_id)
                    .qpath_res(path, expression.hir_id)
                    && let Some(values) = self.facts.values.get(&id)
                {
                    return !values.is_empty() && values.iter().all(|id| {
                        if self.iteration.is_some_and(|iteration| {
                            !self.tcx.hir_parent_iter(*id).any(|(parent, _)| parent == iteration)
                        }) {
                            return false;
                        }
                        matches!(self.tcx.hir_node(*id), hir::Node::Expr(value)
                            if self.productive(value, &mut seen.clone()))
                    });
                }
            }
            ExprKind::Block(block, _) => {
                return block.expr.is_some_and(|tail| self.productive(tail, seen));
            }
            ExprKind::DropTemps(value) | ExprKind::AddrOf(_, _, value) => {
                return self.productive(value, seen);
            }
            ExprKind::Match(condition, _, hir::MatchSource::TryDesugar(_)) => {
                return self.productive(condition, seen);
            }
            ExprKind::Call(_, [value]) => {
                if resolved(self.tcx, expression).is_some_and(|id| {
                    matches!(self.tcx.def_kind(id), DefKind::Ctor(..))
                        || self.tcx.lang_items().branch_fn() == Some(id)
                }) {
                    return self.productive(value, seen);
                }
            }
            ExprKind::MethodCall(_, receiver, _, _)
                if resolved(self.tcx, expression).is_some_and(|id| {
                    let receiver_ty = body_types(self.tcx, receiver.hir_id).expr_ty(receiver).peel_refs();
                    matches!(receiver_ty.kind(), ty::Adt(definition, _)
                        if Some(definition.did()) == self.tcx.lang_items().option_type()
                        || Some(definition.did()) == self.tcx.lang_items().result_ok_variant().map(|id| self.tcx.parent(id)))
                        && matches!(self.tcx.item_name(id).as_str(), "map_err" | "ok")
                }) => {
                    return self.productive(receiver, seen);
            }
            _ => {}
        }
        false
    }

    fn success_pattern(&self, pattern: &hir::Pat<'_>) -> bool {
        if let hir::PatKind::Struct(ref path, fields, _) = pattern.kind {
            let Some(id) = body_types(self.tcx, pattern.hir_id)
                .qpath_res(path, pattern.hir_id)
                .opt_def_id()
            else {
                return false;
            };
            return Some(id) == self.tcx.lang_items().cf_continue_variant()
                && fields.iter().all(|field| {
                    let value = body_types(self.tcx, field.pat.hir_id)
                        .pat_ty(field.pat)
                        .peel_refs();
                    if let ty::Adt(definition, _) = value.kind()
                        && (Some(definition.did()) == self.tcx.lang_items().option_type()
                            || Some(definition.did())
                                == self
                                    .tcx
                                    .lang_items()
                                    .result_ok_variant()
                                    .map(|id| self.tcx.parent(id)))
                    {
                        return self.success_pattern(field.pat);
                    }
                    true
                });
        }
        match pattern.kind {
            hir::PatKind::TupleStruct(ref path, children, _) => {
                let Some(id) = body_types(self.tcx, pattern.hir_id)
                    .qpath_res(path, pattern.hir_id)
                    .opt_def_id()
                else {
                    return false;
                };
                let variant = if matches!(self.tcx.def_kind(id), DefKind::Ctor(..)) {
                    self.tcx.parent(id)
                } else {
                    id
                };
                let lang = self.tcx.lang_items();
                if Some(variant) != lang.option_some_variant()
                    && Some(variant) != lang.result_ok_variant()
                    && Some(variant) != lang.cf_continue_variant()
                {
                    return false;
                }
                children.iter().all(|child| {
                    let value = body_types(self.tcx, child.hir_id).pat_ty(child).peel_refs();
                    if let ty::Adt(definition, _) = value.kind()
                        && (Some(definition.did()) == lang.option_type()
                            || Some(definition.did())
                                == lang.result_ok_variant().map(|id| self.tcx.parent(id)))
                    {
                        return self.success_pattern(child);
                    }
                    true
                })
            }
            hir::PatKind::Or(patterns) => {
                patterns.iter().all(|pattern| self.success_pattern(pattern))
            }
            _ => false,
        }
    }

    fn pattern_progress(&self, value: &Expr<'_>, pattern: &hir::Pat<'_>) -> bool {
        if self.productive(value, &mut HashSet::new()) && self.success_pattern(pattern) {
            return true;
        }
        if let ExprKind::Tup(values) = value.kind
            && let hir::PatKind::Tuple(patterns, _) = pattern.kind
            && values.len() == patterns.len()
        {
            return values
                .iter()
                .zip(patterns)
                .any(|(value, pattern)| self.pattern_progress(value, pattern));
        }
        false
    }

    fn terminal_value(&self, expression: &Expr<'_>) -> bool {
        if is_none(self.tcx, expression) {
            return true;
        }
        if let ExprKind::Call(_, arguments) = expression.kind
            && let Some(id) = resolved(self.tcx, expression)
            && matches!(self.tcx.def_kind(id), DefKind::Ctor(..))
        {
            let variant = self.tcx.parent(id);
            return Some(variant) == self.tcx.lang_items().result_err_variant()
                || (Some(variant) == self.tcx.lang_items().result_ok_variant()
                    && arguments
                        .first()
                        .is_some_and(|value| self.terminal_value(value)));
        }
        resolved(self.tcx, expression)
            .is_some_and(|id| Some(id) == self.tcx.lang_items().from_residual_fn())
    }

    fn returned(&self, paths: &mut Progress, value: Option<&Expr<'_>>) {
        paths.returns |= paths.fallthrough;
        if !value.is_some_and(|value| self.terminal_value(value)) {
            paths.productive_returns |= if paths.fallthrough != 0
                && value.is_some_and(|value| self.productive(value, &mut HashSet::new()))
            {
                2
            } else {
                paths.fallthrough
            };
        }
    }

    fn statements(&self, block: &hir::Block<'_>, input: u8) -> Progress {
        let mut paths = Progress::starting(input);
        for statement in block.stmts {
            match statement.kind {
                hir::StmtKind::Let(local) => {
                    if let Some(init) = local.init {
                        paths.sequence(self.expression(init, paths.fallthrough));
                        local.pat.each_binding(|_, id, _, _| {
                            if self.descent == Some(id) && paths.fallthrough != 0 {
                                paths.fallthrough = 1;
                            }
                        });
                        if let Some(otherwise) = local.els {
                            paths.exits(self.block(otherwise, paths.fallthrough));
                            if self.productive(init, &mut HashSet::new())
                                && self.success_pattern(local.pat)
                                && paths.fallthrough != 0
                            {
                                paths.fallthrough = 2;
                            }
                        }
                    }
                }
                hir::StmtKind::Expr(value) | hir::StmtKind::Semi(value) => {
                    paths.sequence(self.expression(value, paths.fallthrough));
                }
                hir::StmtKind::Item(_) => {}
            }
        }
        paths
    }

    fn block(&self, block: &hir::Block<'_>, input: u8) -> Progress {
        let mut paths = self.statements(block, input);
        if let Some(value) = block.expr {
            paths.sequence(self.expression(value, paths.fallthrough));
        }
        paths.fallthrough |= paths.breaks.remove(&block.hir_id).unwrap_or(0);
        paths
    }

    fn returning(&self, expression: &Expr<'_>, input: u8) -> Progress {
        match expression.kind {
            ExprKind::Block(block, _) => {
                let mut paths = self.statements(block, input);
                if let Some(tail) = block.expr {
                    paths.sequence(self.returning(tail, paths.fallthrough));
                } else {
                    self.returned(&mut paths, None);
                    paths.fallthrough = 0;
                }
                paths
            }
            ExprKind::Match(condition, arms, _) => {
                let mut paths = self.expression(condition, input);
                let incoming = paths.fallthrough;
                paths.fallthrough = 0;
                for arm in arms {
                    let proven = self.pattern_progress(condition, arm.pat);
                    let mut branch =
                        Progress::starting(if proven && incoming != 0 { 2 } else { incoming });
                    if let Some(guard) = arm.guard {
                        branch.sequence(self.expression(guard, branch.fallthrough));
                    }
                    branch.sequence(self.returning(arm.body, branch.fallthrough));
                    paths.exits(branch);
                }
                paths
            }
            ExprKind::If(condition, yes, Some(no)) => {
                let mut paths = self.expression(condition, input);
                paths.exits(self.returning(yes, paths.fallthrough));
                paths.exits(self.returning(no, paths.fallthrough));
                paths.fallthrough = 0;
                paths
            }
            _ => {
                let mut paths = self.expression(expression, input);
                self.returned(&mut paths, Some(expression));
                paths.fallthrough = 0;
                paths
            }
        }
    }

    fn expression(&self, expression: &Expr<'_>, input: u8) -> Progress {
        let mut paths = Progress::starting(input);
        if input == 0 {
            return paths;
        }
        match expression.kind {
            ExprKind::Block(block, _) => return self.block(block, input),
            ExprKind::If(condition, yes, no) => {
                paths = self.expression(condition, input);
                let mut yes_input = paths.fallthrough;
                if let ExprKind::Let(binding) = condition.kind
                    && self.productive(binding.init, &mut HashSet::new())
                    && self.success_pattern(binding.pat)
                    && yes_input != 0
                {
                    yes_input = 2;
                }
                let yes = self.expression(yes, yes_input);
                let no = no.map_or_else(
                    || Progress::starting(paths.fallthrough),
                    |no| self.expression(no, paths.fallthrough),
                );
                paths.fallthrough = yes.fallthrough | no.fallthrough;
                paths.exits(yes);
                paths.exits(no);
            }
            ExprKind::Match(condition, arms, _) => {
                paths = self.expression(condition, input);
                let incoming = paths.fallthrough;
                paths.fallthrough = 0;
                for arm in arms {
                    let proven = self.pattern_progress(condition, arm.pat);
                    let mut branch =
                        Progress::starting(if proven && incoming != 0 { 2 } else { incoming });
                    if let Some(guard) = arm.guard {
                        branch.sequence(self.expression(guard, branch.fallthrough));
                    }
                    branch.sequence(self.expression(arm.body, branch.fallthrough));
                    paths.fallthrough |= branch.fallthrough;
                    paths.exits(branch);
                }
            }
            ExprKind::Loop(block, ..) => {
                let mut entry = input;
                let iteration = Flow {
                    tcx: self.tcx,
                    facts: self.facts,
                    effects: self.effects,
                    iteration: Some(expression.hir_id),
                    descent: self.descent,
                    preparation: self.preparation,
                };
                loop {
                    paths = iteration.block(block, entry);
                    let backedge =
                        paths.fallthrough | paths.continues.remove(&expression.hir_id).unwrap_or(0);
                    let next = entry | backedge;
                    if next == entry {
                        break;
                    }
                    entry = next;
                }
                paths.fallthrough = paths.breaks.remove(&expression.hir_id).unwrap_or(0);
            }
            ExprKind::Ret(value) => {
                if let Some(value) = value {
                    paths = self.expression(value, input);
                }
                self.returned(&mut paths, value);
                paths.fallthrough = 0;
            }
            ExprKind::Break(destination, value) => {
                if let Some(value) = value {
                    paths = self.expression(value, input);
                }
                if let Ok(target) = destination.target_id {
                    *paths.breaks.entry(target).or_default() |= paths.fallthrough;
                }
                paths.fallthrough = 0;
            }
            ExprKind::Continue(destination) => {
                if let Ok(target) = destination.target_id {
                    paths.continues.insert(target, input);
                }
                paths.fallthrough = 0;
            }
            ExprKind::Call(callee, arguments) | ExprKind::MethodCall(_, callee, arguments, _) => {
                paths = self.expression(callee, input);
                for argument in arguments {
                    paths.sequence(self.expression(argument, paths.fallthrough));
                }
                if let Some(preparation) = self.preparation {
                    if expression.hir_id == preparation.poll {
                        paths.target_inputs |= paths.fallthrough;
                    }
                    if std::iter::once(callee).chain(arguments).any(|value| {
                        matches!(
                            body_types(self.tcx, value.hir_id)
                                .expr_ty_adjusted(value)
                                .kind(),
                            ty::Ref(_, _, hir::Mutability::Mut)
                        ) && Place::of(self.tcx, value)
                            .is_some_and(|place| preparation.owner.overlaps(&place))
                    }) && paths.fallthrough != 0
                    {
                        paths.fallthrough = 1;
                    }
                }
                if paths.fallthrough != 0 && self.event_wait(expression) {
                    paths.fallthrough = 2;
                }
                if body_types(self.tcx, expression.hir_id)
                    .expr_ty(expression)
                    .is_never()
                {
                    paths.fallthrough = 0;
                }
            }
            ExprKind::Binary(operator, left, right) => {
                paths = self.expression(left, input);
                let short = matches!(operator.node, hir::BinOpKind::And | hir::BinOpKind::Or)
                    .then_some(paths.fallthrough)
                    .unwrap_or(0);
                paths.sequence(self.expression(right, paths.fallthrough));
                paths.fallthrough |= short;
            }
            ExprKind::Assign(left, right, _) => {
                paths = self.expression(left, input);
                paths.sequence(self.expression(right, paths.fallthrough));
                if let Some(preparation) = self.preparation
                    && Place::of(self.tcx, left)
                        .is_some_and(|place| preparation.owner.overlaps(&place))
                    && paths.fallthrough != 0
                {
                    paths.fallthrough = 1;
                }
                if let Some(cursor) = self.descent
                    && local_binding(self.tcx, left) == Some(cursor)
                    && paths.fallthrough != 0
                {
                    paths.fallthrough = if parent_step(self.tcx, right, cursor) {
                        2
                    } else {
                        1
                    };
                }
            }
            ExprKind::AssignOp(_, left, right) | ExprKind::Index(left, right, _) => {
                paths = self.expression(left, input);
                paths.sequence(self.expression(right, paths.fallthrough));
            }
            ExprKind::Let(binding) => return self.expression(binding.init, input),
            ExprKind::DropTemps(value)
            | ExprKind::Unary(_, value)
            | ExprKind::AddrOf(_, _, value)
            | ExprKind::Cast(value, _)
            | ExprKind::Type(value, _)
            | ExprKind::Field(value, _)
            | ExprKind::Use(value, _) => return self.expression(value, input),
            ExprKind::Array(values) | ExprKind::Tup(values) => {
                for value in values {
                    paths.sequence(self.expression(value, paths.fallthrough));
                }
            }
            ExprKind::Struct(_, fields, _) => {
                for field in fields {
                    paths.sequence(self.expression(field.expr, paths.fallthrough));
                }
            }
            ExprKind::Closure(closure) => {
                if let Some(preparation) = self.preparation
                    && closure_local_uses(self.tcx, closure.body)
                        .contains(&preparation.owner.binding)
                    && paths.fallthrough != 0
                {
                    paths.fallthrough = 1;
                }
            }
            _ => {}
        }
        paths
    }
}

fn effects(tcx: TyCtxt<'_>, facts: &Facts) -> Effects {
    let mut effects = Effects {
        prepared: prepared_polls(tcx, facts),
        ..Effects::default()
    };
    loop {
        let mut always = HashSet::new();
        let mut productive = HashSet::new();
        let flow = Flow {
            tcx,
            facts,
            effects: &effects,
            iteration: None,
            descent: None,
            preparation: None,
        };
        for owner in tcx.hir_body_owners() {
            if !matches!(tcx.def_kind(owner), DefKind::Fn | DefKind::AssocFn) {
                continue;
            }
            let body = tcx.hir_body_owned_by(owner);
            let paths = flow.returning(body.value, 1);
            if paths.returns == 2 {
                always.insert(owner.to_def_id());
            }
            if paths.productive_returns == 2 {
                productive.insert(owner.to_def_id());
            }
        }
        let before = effects.always.len() + effects.productive.len();
        effects.always.extend(always);
        effects.productive.extend(productive);
        if before == effects.always.len() + effects.productive.len() {
            break;
        }
    }
    if let Some(exact) = tcx.all_traits_including_private().find(|id| {
        tcx.crate_name(id.krate).as_str() == "core"
            && tcx.item_name(*id).as_str() == "ExactSizeIterator"
    }) {
        struct Finite<'a, 'tcx> {
            tcx: TyCtxt<'tcx>,
            exact: DefId,
            loops: &'a mut HashMap<HirId, HashSet<HirId>>,
        }
        impl<'tcx> Visitor<'tcx> for Finite<'_, 'tcx> {
            fn visit_expr(&mut self, expression: &'tcx Expr<'tcx>) {
                if let ExprKind::Loop(block, _, hir::LoopSource::ForLoop, _) = expression.kind {
                    let next = block.expr.or_else(|| {
                        block
                            .stmts
                            .iter()
                            .find_map(|statement| match statement.kind {
                                hir::StmtKind::Expr(value) | hir::StmtKind::Semi(value) => {
                                    Some(value)
                                }
                                _ => None,
                            })
                    });
                    if let Some(Expr {
                        kind: ExprKind::Match(next, arms, ..),
                        ..
                    }) = next
                        && let ExprKind::Call(_, [iterator]) = next.kind
                    {
                        let ty = body_types(self.tcx, iterator.hir_id)
                            .expr_ty(iterator)
                            .peel_refs();
                        let (infcx, param_env) = self.tcx.infer_ctxt().build_with_typing_env(
                            ty::TypingEnv::non_body_analysis(
                                self.tcx,
                                self.tcx.hir_enclosing_body_owner(iterator.hir_id),
                            ),
                        );
                        if type_known_to_meet_bound_modulo_regions(
                            &infcx, param_env, ty, self.exact,
                        ) {
                            let mut bindings = HashSet::new();
                            for arm in *arms {
                                arm.pat.each_binding(|_, id, _, _| {
                                    bindings.insert(id);
                                });
                            }
                            self.loops.insert(expression.hir_id, bindings);
                        }
                    }
                }
                intravisit::walk_expr(self, expression);
            }
        }
        for owner in tcx.hir_body_owners() {
            Finite {
                tcx,
                exact,
                loops: &mut effects.finite,
            }
            .visit_body(tcx.hir_body_owned_by(owner));
        }
    }
    let mut descents = HashMap::new();
    for owner in tcx.hir_body_owners() {
        AncestorLoops {
            tcx,
            facts,
            effects: &effects,
            finite: &mut descents,
        }
        .visit_body(tcx.hir_body_owned_by(owner));
    }
    for (id, cursors) in descents {
        effects.finite.entry(id).or_default().extend(cursors);
    }
    effects
}

struct AncestorLoops<'a, 'tcx> {
    tcx: TyCtxt<'tcx>,
    facts: &'a Facts,
    effects: &'a Effects,
    finite: &'a mut HashMap<HirId, HashSet<HirId>>,
}

impl<'tcx> Visitor<'tcx> for AncestorLoops<'_, 'tcx> {
    fn visit_expr(&mut self, expression: &'tcx Expr<'tcx>) {
        if let ExprKind::Loop(block, ..) = expression.kind {
            struct Candidates<'tcx> {
                tcx: TyCtxt<'tcx>,
                cursors: HashSet<HirId>,
                borrowed: HashSet<HirId>,
            }
            impl<'tcx> Visitor<'tcx> for Candidates<'tcx> {
                fn visit_expr(&mut self, expression: &'tcx Expr<'tcx>) {
                    if let ExprKind::Assign(left, right, _) = expression.kind
                        && let Some(cursor) = local_binding(self.tcx, left)
                        && parent_step(self.tcx, right, cursor)
                    {
                        self.cursors.insert(cursor);
                    }
                    if let ExprKind::AddrOf(_, hir::Mutability::Mut, value) = expression.kind
                        && let Some(cursor) = local_binding(self.tcx, value)
                    {
                        self.borrowed.insert(cursor);
                    }
                    if let Some(cursor) = local_binding(self.tcx, expression)
                        && matches!(
                            body_types(self.tcx, expression.hir_id)
                                .expr_ty_adjusted(expression)
                                .kind(),
                            ty::Ref(_, _, hir::Mutability::Mut)
                        )
                    {
                        self.borrowed.insert(cursor);
                    }
                    if let ExprKind::Closure(closure) = expression.kind {
                        // Closure invocation effects are not modeled by the descent proof.
                        self.borrowed
                            .extend(closure_local_uses(self.tcx, closure.body));
                    }
                    intravisit::walk_expr(self, expression);
                }
            }
            let mut candidates = Candidates {
                tcx: self.tcx,
                cursors: HashSet::new(),
                borrowed: HashSet::new(),
            };
            candidates.visit_block(block);
            for cursor in candidates.cursors.difference(&candidates.borrowed) {
                let paths = Flow {
                    tcx: self.tcx,
                    facts: self.facts,
                    effects: self.effects,
                    iteration: Some(expression.hir_id),
                    descent: Some(*cursor),
                    preparation: None,
                }
                .block(block, 1);
                let backedge = paths.fallthrough
                    | paths
                        .continues
                        .get(&expression.hir_id)
                        .copied()
                        .unwrap_or(0);
                if backedge == 2 {
                    self.finite
                        .entry(expression.hir_id)
                        .or_default()
                        .insert(*cursor);
                }
            }
        }
        intravisit::walk_expr(self, expression);
    }
}

fn closure_local_uses(tcx: TyCtxt<'_>, body: hir::BodyId) -> HashSet<HirId> {
    struct Uses<'tcx> {
        tcx: TyCtxt<'tcx>,
        locals: HashSet<HirId>,
    }
    impl<'tcx> Visitor<'tcx> for Uses<'tcx> {
        fn visit_expr(&mut self, expression: &'tcx Expr<'tcx>) {
            if let Some(id) = local_binding(self.tcx, expression) {
                self.locals.insert(id);
            }
            if let ExprKind::Closure(closure) = expression.kind {
                self.visit_body(self.tcx.hir_body(closure.body));
            }
            intravisit::walk_expr(self, expression);
        }
    }
    let mut uses = Uses {
        tcx,
        locals: HashSet::new(),
    };
    uses.visit_body(tcx.hir_body(body));
    uses.locals
}

fn is_none(tcx: TyCtxt<'_>, expression: &Expr<'_>) -> bool {
    resolved(tcx, expression).is_some_and(|id| {
        let variant = if matches!(tcx.def_kind(id), DefKind::Ctor(..)) {
            tcx.parent(id)
        } else {
            id
        };
        Some(variant) == tcx.lang_items().option_none_variant()
    })
}

fn prepared_polls(tcx: TyCtxt<'_>, facts: &Facts) -> HashSet<HirId> {
    struct Polls<'tcx> {
        tcx: TyCtxt<'tcx>,
        candidates: Vec<Preparation>,
    }
    impl<'tcx> Visitor<'tcx> for Polls<'tcx> {
        fn visit_expr(&mut self, expression: &'tcx Expr<'tcx>) {
            if resolved(self.tcx, expression).is_some_and(|id| {
                native_method(
                    self.tcx,
                    id,
                    "libpulse_binding::mainloop::standard::Mainloop",
                    "poll",
                )
            }) && let Some(owner) =
                call_receiver(expression).and_then(|receiver| Place::of(self.tcx, receiver))
            {
                self.candidates.push(Preparation {
                    owner,
                    poll: expression.hir_id,
                });
            }
            intravisit::walk_expr(self, expression);
        }
    }
    let empty = Effects::default();
    let mut prepared = HashSet::new();
    for owner in tcx.hir_body_owners() {
        let body = tcx.hir_body_owned_by(owner);
        let mut polls = Polls {
            tcx,
            candidates: Vec::new(),
        };
        polls.visit_body(body);
        for preparation in polls.candidates {
            let flow = Flow {
                tcx,
                facts,
                effects: &empty,
                iteration: None,
                descent: None,
                preparation: Some(&preparation),
            };
            if flow.returning(body.value, 1).target_inputs == 2 {
                prepared.insert(preparation.poll);
            }
        }
    }
    prepared
}

fn scalar(tcx: TyCtxt<'_>, expression: &Expr<'_>) -> Option<u128> {
    if let ExprKind::Lit(literal) = expression.kind
        && let LitKind::Int(value, _) = literal.node
    {
        return Some(value.get());
    }
    if let ExprKind::Unary(hir::UnOp::Neg, value) = expression.kind {
        return scalar(tcx, value).map(|value| 0u128.wrapping_sub(value));
    }
    let id = resolved(tcx, expression)?;
    if !matches!(
        tcx.def_kind(id),
        DefKind::Const { .. } | DefKind::AssocConst { .. }
    ) {
        return None;
    }
    let value = tcx.const_eval_poly(id).ok()?.try_to_scalar_int()?;
    Some(value.to_bits(value.size()))
}

fn in_loop(tcx: TyCtxt<'_>, id: HirId) -> bool {
    tcx.hir_parent_iter(id).any(|(_, node)| match node {
        hir::Node::Expr(Expr {
            kind: ExprKind::Loop(..),
            ..
        }) => true,
        hir::Node::Expr(Expr {
            kind: ExprKind::Closure(closure),
            ..
        }) => !matches!(
            tcx.coroutine_kind(closure.def_id),
            Some(hir::CoroutineKind::Desugared(
                hir::CoroutineDesugaring::Async,
                hir::CoroutineSource::Fn | hir::CoroutineSource::Block,
            ))
        ),
        _ => false,
    })
}

impl<'tcx> Visitor<'tcx> for CheckBody<'_, 'tcx> {
    fn visit_expr(&mut self, expression: &'tcx Expr<'tcx>) {
        if let ExprKind::Loop(block, ..) = expression.kind {
            let paths = Flow {
                tcx: self.tcx,
                facts: self.facts,
                effects: self.effects,
                iteration: Some(expression.hir_id),
                descent: None,
                preparation: None,
            }
            .block(block, 1);
            let backedge = paths.fallthrough
                | paths
                    .continues
                    .get(&expression.hir_id)
                    .copied()
                    .unwrap_or(0);
            self.polling.push((expression.hir_id, backedge & 1 != 0));
            self.loops += 1;
            intravisit::walk_expr(self, expression);
            self.loops -= 1;
            self.polling.pop();
            return;
        }
        match expression.kind {
            ExprKind::If(condition, then, otherwise) => {
                let mut exits = Exits(false);
                exits.visit_expr(then);
                if let Some(otherwise) = otherwise {
                    exits.visit_expr(otherwise);
                }
                self.condition(condition, exits.0);
            }
            ExprKind::Match(condition, arms, source) => {
                let mut exits = Exits(false);
                for arm in arms {
                    exits.visit_expr(arm.body);
                }
                let presence = optional(self.tcx, condition)
                    && arms
                        .iter()
                        .all(|arm| arm.guard.is_none() && presence_pattern(arm.pat))
                    && self.facts.get(condition) & EXPIRY == 0;
                // An `.await` loop repeats on `Poll::Pending`, which the
                // future's own `poll` decides; the data the future was built
                // from does not control it. Timed futures are rejected where
                // they are created (`timed_wait`).
                let awaited = matches!(source, hir::MatchSource::AwaitDesugar);
                if !presence
                    && !awaited
                    && (!matches!(source, hir::MatchSource::TryDesugar(_))
                        || self.facts.get(condition) & EXPIRY != 0)
                {
                    self.condition(condition, exits.0);
                }
            }
            ExprKind::Path(..) | ExprKind::MethodCall(..) => {
                if let Some(id) = resolved(self.tcx, expression)
                    && matches!(self.tcx.def_kind(id), DefKind::Fn | DefKind::AssocFn)
                {
                    let request_deadline = !in_loop(self.tcx, expression.hir_id)
                        && self
                            .tcx
                            .hir_parent_iter(expression.hir_id)
                            .next()
                            .is_some_and(|(_, parent)| {
                                matches!(
                                    parent,
                                    hir::Node::Expr(Expr {
                                        kind: ExprKind::Call(callee, [deadline]),
                                        ..
                                    }) if callee.hir_id == expression.hir_id
                                        && self.facts.get(deadline) & (CLOCK | REQUEST_BUDGET)
                                            == (CLOCK | REQUEST_BUDGET)
                                        && {
                                            let mut provenance = RequestProvenance {
                                                tcx: self.tcx,
                                                facts: self.facts,
                                                seen: HashSet::new(),
                                                valid: true,
                                            };
                                            provenance.visit_expr(deadline);
                                            provenance.valid
                                        }
                                )
                            });
                    self.item_reference(id, expression.span, request_deadline);
                }
            }
            ExprKind::Lit(literal) => {
                let value = match literal.node {
                    LitKind::Str(value, _) => Some(value.to_string()),
                    LitKind::ByteStr(value, _) => std::str::from_utf8(value.as_byte_str())
                        .ok()
                        .map(str::to_owned),
                    _ => None,
                };
                if let Some(value) = value
                    && (generation_literal(&value)
                        || (value.chars().all(|character| {
                            character.is_ascii_alphanumeric() || character == '_'
                        }) && generation_field(&value)
                            && self.key_context(expression)))
                    && !self.external_identifier(expression, &value)
                {
                    error(
                        self.tcx,
                        expression.span,
                        "contract_generation",
                        "internal route/format markers must not carry a numbered generation",
                    );
                }
            }
            _ => {}
        }
        if let Some(id) = resolved(self.tcx, expression) {
            match expression.kind {
                ExprKind::Call(_, arguments) => self.wait_arguments(expression, id, arguments),
                ExprKind::MethodCall(_, _, arguments, _) => {
                    self.wait_arguments(expression, id, arguments)
                }
                _ => {}
            }
        }
        if let ExprKind::Call(callee, arguments) = expression.kind
            && resolved(self.tcx, expression).is_none()
        {
            for id in self
                .facts
                .functions
                .get(&callee.hir_id)
                .into_iter()
                .flatten()
            {
                self.wait_arguments(expression, *id, arguments);
            }
        }
        intravisit::walk_expr(self, expression);
    }
}

pub fn check(tcx: TyCtxt<'_>) {
    tcx.hir_visit_all_item_likes_in_crate(&mut Declarations { tcx });
    let mut facts = Facts::default();
    loop {
        facts.changed = false;
        for owner in tcx.hir_body_owners() {
            let body = tcx.hir_body_owned_by(owner);
            Propagate {
                tcx,
                facts: &mut facts,
                owner: owner.to_def_id(),
            }
            .visit_body(body);
            facts.returned(
                owner.to_def_id(),
                returned_flags(tcx, owner.to_def_id(), facts.get(body.value)),
            );
        }
        if !facts.changed {
            break;
        }
    }
    let effects = effects(tcx, &facts);
    let api_labels = CheckBody::external_api_labels(tcx);
    for owner in tcx.hir_body_owners() {
        CheckBody {
            tcx,
            facts: &facts,
            loops: 0,
            polling: Vec::new(),
            effects: &effects,
            api_labels: &api_labels,
        }
        .visit_body(tcx.hir_body_owned_by(owner));
    }
    check_lint_overrides(tcx);
}

// Lints the wrapper denies for every owned compilation, and the groups that
// contain them. Generated code (derive and other macro expansions) may carry
// such attributes; hand-written source may not switch the backstop off.
const BACKSTOP_LINTS: &[&str] = &[
    "clippy::disallowed_methods",
    "clippy::disallowed_types",
    "clippy::style",
    "clippy::all",
    "warnings",
];

fn check_lint_overrides(tcx: TyCtxt<'_>) {
    for owner in tcx.hir_crate_items(()).owners() {
        for attributes in tcx.hir_attr_map(owner).map.values() {
            for attribute in attributes.iter() {
                let level = attribute.name();
                if !matches!(level, Some(name) if name == rustc_span::sym::allow || name == rustc_span::sym::expect)
                    || attribute.span().from_expansion()
                {
                    continue;
                }
                for lint in attribute.meta_item_list().into_iter().flatten() {
                    let Some(item) = lint.meta_item() else {
                        continue;
                    };
                    let name = item
                        .path
                        .segments
                        .iter()
                        .map(|segment| segment.ident.as_str().to_owned())
                        .collect::<Vec<_>>()
                        .join("::");
                    if BACKSTOP_LINTS.contains(&name.as_str()) {
                        error(
                            tcx,
                            item.span,
                            "lint_override",
                            "the disallowed-method/type backstop cannot be allowed or expected",
                        );
                    }
                }
            }
        }
    }
}
