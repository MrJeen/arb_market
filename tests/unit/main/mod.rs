use super::*;
fn parse(args: &[&str]) -> Result<Command, &'static str> {
    parse_args(&args.iter().map(OsString::from).collect::<Vec<_>>())
}
#[test]
fn strict_routing() {
    assert_eq!(parse(&[]), Ok(Command::Serve));
    assert_eq!(parse(&["--help"]), Ok(Command::Help));
    assert_eq!(
        parse(&["recompute-actuals", "--confirm"]),
        Ok(Command::RecomputeActuals)
    );
    for args in [
        vec!["recompute-actuals"],
        vec!["--confirm"],
        vec!["unknown"],
        vec!["recompute-actuals", "--help"],
        vec!["--help", "extra"],
        vec!["recompute-actuals", "--confirm", "--confirm"],
        vec!["recompute-actuals", "--confirm", "extra"],
        vec!["--confirm", "recompute-actuals"],
    ] {
        assert!(parse(&args).is_err(), "{args:?}");
    }
}
#[cfg(unix)]
#[test]
fn invalid_unicode_is_rejected_without_panicking() {
    use std::os::unix::ffi::OsStringExt;
    assert!(parse_args(&[OsString::from_vec(vec![255])]).is_err());
}
