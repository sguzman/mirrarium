use std::{
    env,
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

use anyhow::{Context, Result};
use mirrarium_cache as cache;
use mirrarium_corpus as corpus;
use mirrarium_store::{default_data_root, migrate_private_storage, CaptureStore};

const NATIVE_HOST_NAME: &str = "com.sguzman.mirrarium";
const EXTENSION_ID: &str = "oodcefibmdmabgepkcpanjpjolnbignk";

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

    if arguments.first().map(String::as_str) == Some("native-host") {
        return handle_native_host(&arguments[1..]);
    }

    let root = default_data_root()?;

    if arguments.first().map(String::as_str) == Some("privacy")
        && arguments.get(1).map(String::as_str) == Some("migrate")
    {
        println!(
            "{}",
            serde_json::to_string_pretty(&migrate_private_storage(&root)?)?
        );
        return Ok(());
    }

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
        Some("privacy") => match arguments.get(1).map(String::as_str) {
            Some("status") => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&store.private_storage_status()?)?
                );
            }
            Some("migrate") => unreachable!("privacy migrate is handled before store open"),
            Some(command) => anyhow::bail!(
                "unknown privacy command {command:?}; use status or migrate"
            ),
            None => anyhow::bail!(
                "missing privacy command; use 'mirrarium privacy status' or 'mirrarium privacy migrate'"
            ),
        },
        Some("cache") => match arguments.get(1).map(String::as_str) {
            Some("opportunities") => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&cache::opportunities(&root)?)?
                );
            }
            Some("stats") => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&cache::stats(&root)?)?
                );
            }
            Some("replay-stats") => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&cache::replay_stats(&root)?)?
                );
            }
            Some("revalidation-stats") => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&cache::private_revalidation_stats(&root)?)?
                );
            }
            Some("public-coverage") => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&cache::public_coverage(&root)?)?
                );
            }
            Some("private-coverage") => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&cache::private_coverage(&root)?)?
                );
            }
            Some("private-reads") => {
                let limit = arguments
                    .get(2)
                    .map(|value| value.parse::<u64>())
                    .transpose()
                    .context("private-read limit must be a positive integer")?
                    .unwrap_or(200);
                anyhow::ensure!(limit > 0, "private-read limit must be greater than zero");
                println!(
                    "{}",
                    serde_json::to_string_pretty(&cache::private_reads(&root, limit)?)?
                );
            }
            Some("candidates") => {
                let limit = arguments
                    .get(2)
                    .map(|value| value.parse::<u64>())
                    .transpose()
                    .context("cache candidate limit must be a positive integer")?
                    .unwrap_or(200);
                anyhow::ensure!(limit > 0, "cache candidate limit must be greater than zero");
                println!(
                    "{}",
                    serde_json::to_string_pretty(&cache::candidates(&root, limit)?)?
                );
            }
            Some(command) => anyhow::bail!(
                "unknown cache command {command:?}; use opportunities, stats, replay-stats, revalidation-stats, candidates, public-coverage, private-reads, or private-coverage"
            ),
            None => anyhow::bail!(
                "missing cache command; use 'mirrarium cache opportunities', 'mirrarium cache stats', 'mirrarium cache replay-stats', 'mirrarium cache revalidation-stats', 'mirrarium cache candidates', 'mirrarium cache public-coverage', 'mirrarium cache private-reads', or 'mirrarium cache private-coverage'"
            ),
        },
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
            Some("canonical") => {
                let conversation_id = arguments
                    .get(2)
                    .context("missing conversation id")?;
                let view = corpus::canonical(&root, conversation_id)?
                    .with_context(|| {
                        format!(
                            "conversation {conversation_id:?} has no JSON snapshot for canonicalization"
                        )
                    })?;
                println!("{}", serde_json::to_string_pretty(&view)?);
            }
            Some("attachments") => {
                let conversation_id = arguments.get(2).map(String::as_str);
                let limit = arguments
                    .get(3)
                    .map(|value| value.parse::<u64>())
                    .transpose()
                    .context("attachment limit must be a positive integer")?
                    .unwrap_or(200);
                anyhow::ensure!(limit > 0, "attachment limit must be greater than zero");
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &corpus::attachments(&root, conversation_id, limit)?
                    )?
                );
            }
            Some("stream-revisions") => {
                let conversation_id = arguments
                    .get(2)
                    .context("missing conversation id")?;
                let limit = arguments
                    .get(3)
                    .map(|value| value.parse::<u64>())
                    .transpose()
                    .context("stream revision limit must be a positive integer")?
                    .unwrap_or(500);
                anyhow::ensure!(limit > 0, "stream revision limit must be greater than zero");
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &corpus::stream_message_revisions(&root, conversation_id, limit)?
                    )?
                );
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


fn handle_native_host(arguments: &[String]) -> Result<()> {
    match arguments.first().map(String::as_str) {
        Some("install") => {
            let browser = arguments.get(1).map(String::as_str).unwrap_or("edge");
            let host_path = resolve_native_host_binary(arguments.get(2).map(String::as_str))?;
            let manifest_path = install_native_host(browser, &host_path)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "browser": browser,
                    "manifest_path": manifest_path,
                    "host_path": host_path,
                    "extension_id": EXTENSION_ID,
                }))?
            );
        }
        Some("status") => {
            let browser = arguments.get(1).map(String::as_str).unwrap_or("edge");
            let manifest_path = native_host_manifest_path(browser)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "browser": browser,
                    "manifest_path": manifest_path,
                    "installed": manifest_path.is_file(),
                }))?
            );
        }
        Some("uninstall") => {
            let browser = arguments.get(1).map(String::as_str).unwrap_or("edge");
            let manifest_path = native_host_manifest_path(browser)?;
            let removed = if manifest_path.is_file() {
                fs::remove_file(&manifest_path)
                    .with_context(|| format!("removing {}", manifest_path.display()))?;
                true
            } else {
                false
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "browser": browser,
                    "manifest_path": manifest_path,
                    "removed": removed,
                }))?
            );
        }
        Some(command) => anyhow::bail!(
            "unknown native-host command {command:?}; use install, status, or uninstall"
        ),
        None => anyhow::bail!(
            "missing native-host command; use 'mirrarium native-host install'"
        ),
    }

    Ok(())
}

fn install_native_host(browser: &str, host_path: &Path) -> Result<PathBuf> {
    let manifest_path = native_host_manifest_path(browser)?;
    let parent = manifest_path
        .parent()
        .context("native-host manifest path has no parent")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("creating {}", parent.display()))?;

    let manifest = serde_json::json!({
        "name": NATIVE_HOST_NAME,
        "description": "Mirrarium native messaging host",
        "path": host_path.to_string_lossy(),
        "type": "stdio",
        "allowed_origins": [format!("chrome-extension://{EXTENSION_ID}/")],
    });
    let bytes = serde_json::to_vec_pretty(&manifest)?;
    let temp_path = parent.join(format!(".{NATIVE_HOST_NAME}.{}.tmp", std::process::id()));

    fs::write(&temp_path, bytes)
        .with_context(|| format!("writing {}", temp_path.display()))?;
    harden_manifest_file(&temp_path)?;
    fs::rename(&temp_path, &manifest_path).with_context(|| {
        format!(
            "moving {} to {}",
            temp_path.display(),
            manifest_path.display()
        )
    })?;
    harden_manifest_file(&manifest_path)?;

    Ok(manifest_path)
}

fn native_host_manifest_path(browser: &str) -> Result<PathBuf> {
    let user_data_dir = if let Some(path) = env::var_os("MIRRARIUM_BROWSER_USER_DATA_DIR") {
        PathBuf::from(path)
    } else {
        let config_root = if let Some(path) = env::var_os("XDG_CONFIG_HOME") {
            PathBuf::from(path)
        } else {
            let home = env::var_os("HOME")
                .context("set HOME, XDG_CONFIG_HOME, or MIRRARIUM_BROWSER_USER_DATA_DIR")?;
            PathBuf::from(home).join(".config")
        };
        browser_user_data_dir(browser, &config_root)?
    };

    Ok(user_data_dir
        .join("NativeMessagingHosts")
        .join(format!("{NATIVE_HOST_NAME}.json")))
}

fn browser_user_data_dir(browser: &str, config_root: &Path) -> Result<PathBuf> {
    let directory = match browser {
        "edge" => "microsoft-edge",
        "chromium" => "chromium",
        "chrome" => "google-chrome",
        "chrome-for-testing" => "google-chrome-for-testing",
        other => anyhow::bail!(
            "unsupported browser {other:?}; use edge, chromium, chrome, or chrome-for-testing"
        ),
    };
    Ok(config_root.join(directory))
}

fn resolve_native_host_binary(explicit: Option<&str>) -> Result<PathBuf> {
    let candidate = explicit
        .map(PathBuf::from)
        .or_else(|| env::var_os("MIRRARIUMD_PATH").map(PathBuf::from))
        .unwrap_or_else(|| {
            env::current_exe()
                .map(|path| path.with_file_name("mirrariumd"))
                .unwrap_or_else(|_| PathBuf::from("mirrariumd"))
        });

    anyhow::ensure!(
        candidate.is_file(),
        "native host binary does not exist: {}",
        candidate.display()
    );
    let canonical = fs::canonicalize(&candidate)
        .with_context(|| format!("resolving {}", candidate.display()))?;
    ensure_executable(&canonical)?;
    Ok(canonical)
}

#[cfg(unix)]
fn ensure_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = fs::metadata(path)?.permissions().mode();
    anyhow::ensure!(
        mode & 0o111 != 0,
        "native host is not executable: {}",
        path.display()
    );
    Ok(())
}

#[cfg(not(unix))]
fn ensure_executable(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn harden_manifest_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("hardening {}", path.display()))
}

#[cfg(not(unix))]
fn harden_manifest_file(_path: &Path) -> Result<()> {
    Ok(())
}

fn print_help() {
    println!(
        "Mirrarium local corpus/cache inspector

USAGE:
  mirrarium stats
  mirrarium captures [LIMIT]
  mirrarium verify
  mirrarium privacy status
  mirrarium privacy migrate
  mirrarium cache opportunities
  mirrarium cache stats
  mirrarium cache replay-stats
  mirrarium cache revalidation-stats
  mirrarium cache candidates [LIMIT]
  mirrarium cache public-coverage
  mirrarium cache private-reads [LIMIT]
  mirrarium cache private-coverage
  mirrarium corpus rebuild
  mirrarium corpus stats
  mirrarium corpus conversations [LIMIT]
  mirrarium corpus conversation <ID> [MESSAGE_LIMIT]
  mirrarium corpus canonical <ID>
  mirrarium corpus attachments [CONVERSATION_ID] [LIMIT]
  mirrarium corpus stream-revisions <CONVERSATION_ID> [LIMIT]
  mirrarium native-host install [BROWSER] [MIRRARIUMD_PATH]
  mirrarium native-host status [BROWSER]
  mirrarium native-host uninstall [BROWSER]

NATIVE HOST:
  BROWSER defaults to edge.
  MIRRARIUM_BROWSER_USER_DATA_DIR overrides the browser user-data root.

DATA ROOT:
  MIRRARIUM_DATA_DIR, then XDG_DATA_HOME/mirrarium,
  then ~/.local/share/mirrarium

PRIVATE KEY:
  MIRRARIUM_PRIVATE_KEY_FILE overrides the key path.
  Explicit MIRRARIUM_DATA_DIR instances keep a key under .keys/private.key;
  zero-config installs use XDG_CONFIG_HOME/mirrarium/private.key or
  ~/.config/mirrarium/private.key."
    );
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_manifest_paths_match_linux_user_data_layouts() {
        let root = Path::new("/tmp/config");
        assert_eq!(
            browser_user_data_dir("edge", root).unwrap(),
            root.join("microsoft-edge")
        );
        assert_eq!(
            browser_user_data_dir("chrome-for-testing", root).unwrap(),
            root.join("google-chrome-for-testing")
        );
        assert!(browser_user_data_dir("unknown", root).is_err());
    }
}
