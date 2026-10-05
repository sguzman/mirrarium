use std::{env, process::ExitCode};

use anyhow::{Context, Result};
use mirrarium_corpus as corpus;
use mirrarium_store::{default_data_root, CaptureStore};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mirrarium: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    let root = default_data_root()?;
    let store = CaptureStore::open(&root)?;

    match arguments.first().map(String::as_str) {
        Some("stats") => {
            println!("{}", serde_json::to_string_pretty(&store.stats()?)?);
        }
        Some("captures") => {
            let limit = arguments
                .get(1)
                .map(|value| value.parse::<u64>())
                .transpose()
                .context("capture limit must be a positive integer")?
                .unwrap_or(20);
            anyhow::ensure!(limit > 0, "capture limit must be greater than zero");
            println!(
                "{}",
                serde_json::to_string_pretty(&store.recent_captures(limit)?)?
            );
        }
        Some("verify") => {
            let report = store.verify()?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            anyhow::ensure!(
                report.corrupt_objects == 0,
                "{} corrupt object(s) found",
                report.corrupt_objects
            );
        }
        Some("corpus") => match arguments.get(1).map(String::as_str) {
            Some("rebuild") => {
                println!("{}", serde_json::to_string_pretty(&corpus::rebuild(&root)?)?);
            }
            Some("stats") => {
                println!("{}", serde_json::to_string_pretty(&corpus::stats(&root)?)?);
            }
            Some("conversations") => {
                let limit = arguments
                    .get(2)
                    .map(|value| value.parse::<u64>())
                    .transpose()
                    .context("conversation limit must be a positive integer")?
                    .unwrap_or(50);
                anyhow::ensure!(limit > 0, "conversation limit must be greater than zero");
                println!(
                    "{}",
                    serde_json::to_string_pretty(&corpus::conversations(&root, limit)?)?
                );
            }
            Some("conversation") => {
                let conversation_id = arguments
                    .get(2)
                    .context("missing conversation id")?;
                let limit = arguments
                    .get(3)
                    .map(|value| value.parse::<u64>())
                    .transpose()
                    .context("message observation limit must be a positive integer")?
                    .unwrap_or(200);
                anyhow::ensure!(
                    limit > 0,
                    "message observation limit must be greater than zero"
                );
                let view = corpus::conversation(&root, conversation_id, limit)?
                    .with_context(|| format!("conversation {conversation_id:?} not found"))?;
                println!("{}", serde_json::to_string_pretty(&view)?);
            }
            Some(command) => anyhow::bail!(
                "unknown corpus command {command:?}; run 'mirrarium help'"
            ),
            None => anyhow::bail!("missing corpus command; run 'mirrarium help'"),
        },
        Some("help" | "--help" | "-h") | None => print_help(),
        Some(command) => anyhow::bail!("unknown command {command:?}; run 'mirrarium help'"),
    }

    Ok(())
}

fn print_help() {
    println!(
        "Mirrarium local corpus/cache inspector

USAGE:
  mirrarium stats
  mirrarium captures [LIMIT]
  mirrarium verify
  mirrarium corpus rebuild
  mirrarium corpus stats
  mirrarium corpus conversations [LIMIT]
  mirrarium corpus conversation <ID> [MESSAGE_LIMIT]

DATA ROOT:
  MIRRARIUM_DATA_DIR, then XDG_DATA_HOME/mirrarium,
  then ~/.local/share/mirrarium"
    );
}
