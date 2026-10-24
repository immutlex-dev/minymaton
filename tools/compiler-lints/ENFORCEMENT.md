# Typed compiler enforcement

The driver, policy, wrapper and bootstrap in this directory are complete and
self-contained; the `fogscrib` names in their binaries and diagnostics are the
gate's own identifiers.
Minymaton pins Rust 1.98.0; compiler-private APIs require the matching
`rustc-dev`, `rust-src` and `llvm-tools-preview` components.

```
python3 tools/compiler-lints/bootstrap.py
cargo build
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`.cargo/config.toml` composes the typed driver with Clippy for normal owned
builds. The wrapper checks source and toolchain fingerprints and rejects stale
bootstrap executables. Registry dependencies outside the checkout are not
owned sources. Neither lint caps nor handwritten suppressions bypass the
policy. CI bootstraps and exercises the same path.

The HIR pass rejects resolved timed waits, native finite waits, clock-driven
lifecycle control, readiness polling, internal contract generations and
compatibility surfaces. These are structural recognizers, not a proof of
arbitrary intent. Necessary external MCP protocol fields are serialized from
rmcp's typed handshake contracts instead of introducing internal generations.

One narrow request-boundary recognizer permits a direct Tokio `sleep_until`
construction in the function body or a plain async block outside any enclosing
loop or callable closure when its deadline carries both a clock and
value-preserving request-budget provenance. The request must come from
deserializing an unmodified function input with the real
`serde_json::from_value`. Immutable input aliases, field/index projections,
built-in reference dereferences, non-local `core`/`std`/`serde_json`
`as_u64`/`unwrap`/`expect`/`unwrap_or`/`unwrap_or_default`/`map_err` calls,
`min` capping on `u64` or `Duration`, `Duration::from_millis`, and adding a
`Duration` budget to an unshifted clock (`std::time::Instant::now` /
`tokio::time::Instant::now` or an unmodified parameter/local clock binding) are
followed. Branch joins preserve the budget only when every non-diverging arm
carries it; condition-only dependence, shifted base clocks, arbitrary
arithmetic, compound assignment (`+=`), unary negation/bitwise-not, casts,
custom `Deref`/`Ord`/`Add` overloads, `max` inflation, mutated budget
dependencies, and local or closure helper returns (including ordinary or async
closures that construct a timer) do not authorize a deadline. The timer is
constructed once at admission, then selected alongside commit and ownership
events without resetting or renewing it.

This recognizer does not authenticate arbitrary function parameters, inspect
custom `serde::Deserialize` implementations, or prove cross-crate transport
origins. Review must still establish that the input is the actual external
request, that request types use plain derived deserialization, and that the
duration bounds that request rather than lifecycle correctness. Fixed internal
timers, constructed requests, discarded or replaced budgets, helper-produced
deadlines and loop-renewed timers are not authorized.

`crates/minymaton-server/tests/compiler_policy.rs` invokes the real wrapper,
using compiled Tokio and serde_json dependencies. Positive inputs must emit
metadata. Negative inputs must fail with the expected policy diagnostic and
must not emit metadata. The test also covers native sleeps, internal
generation fields and event-based waits. It runs with the real-server,
write-failure and webhook-delivery integration tests, not a source scanner.

Bootstrap output and executables are ignored under `results/` and `bin/`.
Transfer source to another host and bootstrap there; do not transfer compiler
executables or clear wrappers to get a build through.
