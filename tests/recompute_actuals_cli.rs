//! Subprocesses run with empty environments in private, dotenv-free directories.
use std::process::{Command, Output};

fn invoke(args: &[&str], configure: bool, builder: Option<&str>) -> Output {
    let dir = std::env::temp_dir().join(format!("recompute-cli-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_market-arb"));
    command.current_dir(&dir).env_clear().args(args);
    if configure {
        command
            .env(
                "APP_POSTGRES_URI",
                "postgres://unused:unused@127.0.0.1:1/unused",
            )
            .env("HYPERLIQUID_INFO_URL", "http://127.0.0.1:1/info")
            .env("POLYMARKET_FUNDER_KEYS_FILE", "/must-not-open-funders")
            .env("POLYMARKET_FUNDER_PRIVATE_KEYS", "invalid")
            .env("OUTCOME_AGENT_PRIVATE_KEY", "invalid");
    }
    if let Some(builder) = builder {
        command.env("OUTCOME_BUILDER_ADDRESS", builder);
    }
    let output = command.output().unwrap();
    std::fs::remove_dir(&dir).unwrap();
    output
}

#[test]
fn help_and_invalid_arguments_do_not_initialize() {
    let help = invoke(&["--help"], false, Some("invalid"));
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("Usage:"));
    for args in [
        vec!["recompute-actuals"],
        vec!["recompute-actuals", "--confirm", "--confirm"],
        vec!["--help", "extra"],
        vec!["unknown"],
    ] {
        let result = invoke(&args, false, Some("invalid"));
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
        assert!(String::from_utf8_lossy(&result.stderr).contains("invalid arguments"));
    }
}

#[test]
fn narrow_config_does_not_parse_trading_secrets_or_require_common() {
    // Port 1 is loopback-only; a connect failure proves configuration/client initialization passed.
    let output = invoke(&["recompute-actuals", "--confirm"], true, Some(" OFF "));
    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("database_connection"), "{stdout}");
    assert!(stdout.contains("scanned=0 repaired=0 still_unknown=0 skipped=0 errors=1"));
    assert!(!stdout.contains("postgres://"));
    let invalid = invoke(&["recompute-actuals", "--confirm"], true, Some("invalid"));
    assert!(String::from_utf8_lossy(&invalid.stdout).contains("configuration"));
}
