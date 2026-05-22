//! Compile-fail tests: confirm `#[service]` rejects malformed traits with the
//! expected, span-pointed diagnostics. Regenerate `.stderr` snapshots after an
//! intentional message change with `TRYBUILD=overwrite cargo test -p patina-macros`.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
