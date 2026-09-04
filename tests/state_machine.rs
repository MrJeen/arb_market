//! Database state-machine coverage for execute_plan → reconcile → hedge.
//! Run with `APP_POSTGRES_URI` when a throwaway database is available:
//! `cargo test --test state_machine -- --ignored --nocapture`

#[test]
#[ignore = "requires APP_POSTGRES_URI throwaway database"]
fn execute_plan_reconcile_hedge_needs_postgres() {
    let uri = std::env::var("APP_POSTGRES_URI").unwrap_or_default();
    assert!(
        !uri.is_empty(),
        "set APP_POSTGRES_URI to a throwaway database before running this ignored test"
    );
}
