use std::ffi::OsString;

const HELP: &str = "Usage: market-arb [--help | recompute-actuals --confirm]\n\nNo arguments: start the normal service.\nrecompute-actuals --confirm: one bounded repair of unknown actuals, then exit.\nThis maintenance command writes projections, queries info fees, and never trades or migrates.";

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Serve,
    Help,
    RecomputeActuals,
}

fn parse_args(args: &[OsString]) -> Result<Command, &'static str> {
    match args {
        [] => Ok(Command::Serve),
        [arg] if arg == "--help" => Ok(Command::Help),
        [command, confirm] if command == "recompute-actuals" && confirm == "--confirm" => {
            Ok(Command::RecomputeActuals)
        }
        _ => {
            Err("invalid arguments; expected no arguments, --help, or recompute-actuals --confirm")
        }
    }
}

fn main() -> anyhow::Result<()> {
    // Validate before dotenv, runtime, logging, database or HTTP initialization.
    let command =
        parse_args(&std::env::args_os().skip(1).collect::<Vec<_>>()).map_err(anyhow::Error::msg)?;
    if command == Command::Help {
        println!("{HELP}");
        return Ok(());
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            match command {
                Command::Serve => market_arb::run().await,
                Command::RecomputeActuals => {
                    if market_arb::recompute_actuals::run().await.success() {
                        Ok(())
                    } else {
                        anyhow::bail!(
                            "recompute incomplete; inspect summary and remaining unknown evidence"
                        )
                    }
                }
                Command::Help => unreachable!(),
            }
        })
}

#[cfg(test)]
mod tests {
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
}
