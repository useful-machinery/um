#[derive(Debug, PartialEq)]
struct CompiledChecks {
    debug_assertions: bool,
    overflow_checks: bool,
}

// INV-TESTING-23
#[test]
fn the_suite_is_compiled_with_debug_assertions_and_overflow_checks() {
    // No stable `cfg` reports overflow checks, so observe them: the addition
    // panics only when they are compiled in.
    let overflow =
        std::panic::catch_unwind(|| std::hint::black_box(u8::MAX) + std::hint::black_box(1_u8));
    let compiled = CompiledChecks {
        debug_assertions: cfg!(debug_assertions),
        overflow_checks: overflow.is_err(),
    };

    assert_eq!(
        compiled,
        CompiledChecks {
            debug_assertions: true,
            overflow_checks: true,
        },
        "INV-TESTING-23: the CLI suite is specified for Cargo's `test` profile, \
         with debug assertions and integer-overflow checks compiled in"
    );
}
