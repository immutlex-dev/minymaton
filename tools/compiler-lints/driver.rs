#![feature(rustc_private)]

extern crate rustc_ast;
extern crate rustc_driver;
extern crate rustc_hir;
extern crate rustc_infer;
extern crate rustc_interface;
extern crate rustc_middle;
extern crate rustc_span;
extern crate rustc_trait_selection;

mod policy;

use rustc_driver::{Callbacks, Compilation};
use rustc_interface::interface;
use rustc_middle::ty::TyCtxt;

struct Policy;

impl Callbacks for Policy {
    fn after_analysis<'tcx>(
        &mut self,
        _compiler: &interface::Compiler,
        tcx: TyCtxt<'tcx>,
    ) -> Compilation {
        if let Err(error) = record_sources(tcx) {
            tcx.dcx()
                .err(format!("fogscrib compiler coverage: {error}"));
        }
        policy::check(tcx);
        Compilation::Stop
    }
}

fn record_sources(tcx: TyCtxt<'_>) -> std::io::Result<()> {
    use std::io::Write;
    let Some(directory) = std::env::var_os("FOGSCRIB_LINT_COVERAGE_DIR") else {
        return Ok(());
    };
    let directory = std::path::PathBuf::from(directory);
    std::fs::create_dir_all(&directory)?;
    let name = format!(
        "{}-{}.sources",
        std::process::id(),
        tcx.crate_name(rustc_hir::def_id::LOCAL_CRATE)
    );
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join(name))?;
    writeln!(output, "target:{}", tcx.sess.opts.target_triple)?;
    for source in tcx.sess.source_map().files().iter() {
        if let rustc_span::FileName::Real(name) = &source.name
            && let Some(path) = name.local_path()
        {
            writeln!(output, "{}", std::path::absolute(path)?.display())?;
        }
    }
    Ok(())
}

fn main() {
    rustc_driver::run_compiler(&std::env::args().collect::<Vec<_>>(), &mut Policy);
}
