use std::{
    collections::BTreeSet,
    env,
    ffi::OsString,
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
    if arguments.first().map(String::as_str) == Some("extension") {
        return handle_extension(&arguments[1..]);
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

    match arguments.first().map(String::as_str) {
        Some("stats") => {
            let store = CaptureStore::open_read_only(&root)?;
            println!("{}", serde_json::to_string_pretty(&store.stats()?)?);
        }
        Some("captures") => {
            let store = CaptureStore::open_read_only(&root)?;
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
            let store = CaptureStore::open_read_only(&root)?;
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
                let store = CaptureStore::open_read_only(&root)?;
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



fn handle_extension(arguments: &[String]) -> Result<()> {
    match arguments.first().map(String::as_str) {
        Some("install") => {
            let source = resolve_extension_source(arguments.get(1).map(String::as_str))?;
            let install_path = extension_install_path()?;
            let manifest = install_extension(&source, &install_path)?;
            publish_extension_install_state(&install_path, &manifest)?;
            let build_id = manifest
                .get("version_name")
                .and_then(serde_json::Value::as_str)
                .context("installed extension manifest is missing version_name")?;
            let running_build_id = read_extension_runtime_build_id()?;
            let reload_required = running_build_id
                .as_deref()
                .is_some_and(|running| running != build_id);
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "source_path": source,
                    "install_path": install_path,
                    "extension_id": EXTENSION_ID,
                    "manifest_version": manifest["manifest_version"],
                    "version": manifest["version"],
                    "build_id": build_id,
                    "running_build_id": running_build_id,
                    "reload_required": reload_required,
                }))?
            );
        }
        Some("status") => {
            let install_path = extension_install_path()?;
            let manifest_path = install_path.join("manifest.json");
            let installed = install_path.is_dir();
            let (valid, version, build_id, error) = if installed {
                match validate_extension_directory(&install_path) {
                    Ok(manifest) => (
                        true,
                        manifest.get("version").cloned().unwrap_or(serde_json::Value::Null),
                        manifest
                            .get("version_name")
                            .cloned()
                            .unwrap_or(serde_json::Value::Null),
                        serde_json::Value::Null,
                    ),
                    Err(error) => (
                        false,
                        serde_json::Value::Null,
                        serde_json::Value::Null,
                        serde_json::Value::String(format!("{error:#}")),
                    ),
                }
            } else {
                (
                    false,
                    serde_json::Value::Null,
                    serde_json::Value::Null,
                    serde_json::Value::Null,
                )
            };
            let running_build_id = read_extension_runtime_build_id()?;
            let reload_required = build_id.as_str().is_some_and(|installed_build_id| {
                running_build_id
                    .as_deref()
                    .is_some_and(|running| running != installed_build_id)
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "install_path": install_path,
                    "manifest_path": manifest_path,
                    "installed": installed,
                    "valid": valid,
                    "version": version,
                    "build_id": build_id,
                    "running_build_id": running_build_id,
                    "reload_required": reload_required,
                    "extension_id": EXTENSION_ID,
                    "error": error,
                }))?
            );
        }
        Some("uninstall") => {
            let install_path = extension_install_path()?;
            let removed = if install_path.exists() {
                fs::remove_dir_all(&install_path)
                    .with_context(|| format!("removing {}", install_path.display()))?;
                true
            } else {
                false
            };
            for state_path in [extension_state_path()?, extension_runtime_state_path()?] {
                if state_path.is_file() {
                    fs::remove_file(&state_path)
                        .with_context(|| format!("removing {}", state_path.display()))?;
                }
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "install_path": install_path,
                    "removed": removed,
                }))?
            );
        }
        Some(command) => anyhow::bail!(
            "unknown extension command {command:?}; use install, status, or uninstall"
        ),
        None => anyhow::bail!(
            "missing extension command; use 'mirrarium extension install [SOURCE_DIR]'"
        ),
    }

    Ok(())
}

fn extension_install_path() -> Result<PathBuf> {
    if let Some(path) = env::var_os("MIRRARIUM_EXTENSION_DIR") {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = env::var_os("XDG_DATA_HOME") {
        return Ok(PathBuf::from(path).join("mirrarium/extension"));
    }
    let home = env::var_os("HOME")
        .context("set HOME, XDG_DATA_HOME, or MIRRARIUM_EXTENSION_DIR")?;
    Ok(PathBuf::from(home).join(".local/share/mirrarium/extension"))
}

fn extension_state_path() -> Result<PathBuf> {
    if let Some(path) = env::var_os("MIRRARIUM_EXTENSION_STATE_FILE") {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(path).join("mirrarium/extension-install.json"));
    }
    let home = env::var_os("HOME")
        .context("set HOME, XDG_CONFIG_HOME, or MIRRARIUM_EXTENSION_STATE_FILE")?;
    Ok(PathBuf::from(home).join(".config/mirrarium/extension-install.json"))
}

fn extension_runtime_state_path() -> Result<PathBuf> {
    let install_state_path = extension_state_path()?;
    let parent = install_state_path
        .parent()
        .context("extension install-state path has no parent directory")?;
    Ok(parent.join("extension-runtime.json"))
}

fn read_extension_runtime_build_id() -> Result<Option<String>> {
    let state_path = extension_runtime_state_path()?;
    if !state_path.is_file() {
        return Ok(None);
    }

    let state: serde_json::Value = serde_json::from_slice(
        &fs::read(&state_path)
            .with_context(|| format!("reading extension runtime state {}", state_path.display()))?,
    )
    .with_context(|| format!("parsing extension runtime state {}", state_path.display()))?;

    if state
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        != Some(1)
        || state
            .get("extension_id")
            .and_then(serde_json::Value::as_str)
            != Some(EXTENSION_ID)
    {
        return Ok(None);
    }

    Ok(state
        .get("build_id")
        .and_then(serde_json::Value::as_str)
        .filter(|build_id| !build_id.is_empty())
        .map(str::to_owned))
}

fn publish_extension_install_state(
    install_path: &Path,
    manifest: &serde_json::Value,
) -> Result<PathBuf> {
    let state_path = extension_state_path()?;
    let parent = state_path
        .parent()
        .context("extension state path has no parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("creating extension state directory {}", parent.display()))?;

    let canonical_install = fs::canonicalize(install_path)
        .with_context(|| format!("resolving installed extension {}", install_path.display()))?;
    let build_id = manifest
        .get("version_name")
        .and_then(serde_json::Value::as_str)
        .context("installed extension manifest is missing version_name")?;
    let state = serde_json::json!({
        "schema_version": 1,
        "extension_id": EXTENSION_ID,
        "build_id": build_id,
        "install_path": canonical_install,
    });
    let temp = parent.join(format!(
        ".extension-install.{}.tmp",
        std::process::id()
    ));
    fs::write(&temp, serde_json::to_vec_pretty(&state)?)
        .with_context(|| format!("writing extension state {}", temp.display()))?;
    fs::rename(&temp, &state_path).with_context(|| {
        format!(
            "installing extension state {} to {}",
            temp.display(),
            state_path.display()
        )
    })?;
    Ok(state_path)
}

fn resolve_extension_source(explicit: Option<&str>) -> Result<PathBuf> {
    let candidate = explicit
        .map(PathBuf::from)
        .or_else(|| env::var_os("MIRRARIUM_EXTENSION_SOURCE").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("extension/dist"));
    anyhow::ensure!(
        candidate.is_dir(),
        "extension source directory does not exist: {}",
        candidate.display()
    );
    let canonical = fs::canonicalize(&candidate)
        .with_context(|| format!("resolving extension source {}", candidate.display()))?;
    validate_extension_directory(&canonical)?;
    Ok(canonical)
}

fn validate_extension_directory(path: &Path) -> Result<serde_json::Value> {
    anyhow::ensure!(path.is_dir(), "extension directory does not exist: {}", path.display());
    let manifest_path = path.join("manifest.json");
    let background_path = path.join("background.js");
    anyhow::ensure!(
        manifest_path.is_file(),
        "extension manifest is missing: {}",
        manifest_path.display()
    );
    anyhow::ensure!(
        background_path.is_file(),
        "extension background worker is missing: {}",
        background_path.display()
    );

    let manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(&manifest_path)
            .with_context(|| format!("reading {}", manifest_path.display()))?,
    )
    .with_context(|| format!("parsing {}", manifest_path.display()))?;
    anyhow::ensure!(
        manifest.get("manifest_version").and_then(serde_json::Value::as_u64) == Some(3),
        "Mirrarium extension must use manifest_version 3"
    );
    anyhow::ensure!(
        manifest.get("name").and_then(serde_json::Value::as_str) == Some("Mirrarium"),
        "extension manifest name is not Mirrarium"
    );
    anyhow::ensure!(
        manifest.get("key").and_then(serde_json::Value::as_str).is_some_and(|key| !key.is_empty()),
        "extension manifest is missing its stable key"
    );
    anyhow::ensure!(
        manifest.get("version").and_then(serde_json::Value::as_str).is_some(),
        "extension manifest is missing version"
    );
    anyhow::ensure!(
        manifest
            .get("version_name")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|build_id| !build_id.is_empty()),
        "extension manifest is missing deterministic version_name build id"
    );
    Ok(manifest)
}

fn install_extension(source: &Path, destination: &Path) -> Result<serde_json::Value> {
    let source = fs::canonicalize(source)
        .with_context(|| format!("resolving extension source {}", source.display()))?;
    let source_manifest = validate_extension_directory(&source)?;

    if destination.exists() {
        if let Ok(installed) = fs::canonicalize(destination) {
            if installed == source {
                return Ok(source_manifest);
            }
        }
    }

    let parent = destination
        .parent()
        .context("extension install path has no parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("creating extension install parent {}", parent.display()))?;

    let temp = parent.join(format!(".extension.install.{}.tmp", std::process::id()));
    if temp.exists() {
        fs::remove_dir_all(&temp)
            .with_context(|| format!("removing stale extension staging {}", temp.display()))?;
    }

    copy_extension_tree(&source, &temp)?;
    let installed_manifest = validate_extension_directory(&temp)?;

    if destination.exists() {
        anyhow::ensure!(
            destination.is_dir(),
            "extension install destination is not a directory: {}",
            destination.display()
        );
        activate_staged_extension_directory(&temp, destination, true)?;
    } else {
        fs::rename(&temp, destination).with_context(|| {
            format!(
                "installing extension {} to {}",
                temp.display(),
                destination.display()
            )
        })?;
    }

    Ok(installed_manifest)
}

fn activate_staged_extension_directory(
    staging: &Path,
    destination: &Path,
    manifest_last: bool,
) -> Result<()> {
    fs::create_dir_all(destination)
        .with_context(|| format!("creating extension destination {}", destination.display()))?;

    let mut entries = Vec::new();
    let mut expected_names = BTreeSet::<OsString>::new();
    for entry in fs::read_dir(staging)
        .with_context(|| format!("reading extension staging {}", staging.display()))?
    {
        let entry = entry?;
        expected_names.insert(entry.file_name());
        entries.push(entry);
    }
    entries.sort_by_key(|entry| entry.file_name());

    for entry in &entries {
        if manifest_last && entry.file_name() == OsString::from("manifest.json") {
            continue;
        }
        activate_staged_extension_entry(&entry.path(), &destination.join(entry.file_name()))?;
    }

    if manifest_last {
        let manifest = staging.join("manifest.json");
        anyhow::ensure!(
            manifest.is_file(),
            "validated extension staging lost manifest.json before activation"
        );
        activate_staged_extension_entry(&manifest, &destination.join("manifest.json"))?;
    }

    for entry in fs::read_dir(destination)
        .with_context(|| format!("reading extension destination {}", destination.display()))?
    {
        let entry = entry?;
        if expected_names.contains(&entry.file_name()) {
            continue;
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_dir() {
            fs::remove_dir_all(&path)
                .with_context(|| format!("removing obsolete extension directory {}", path.display()))?;
        } else {
            fs::remove_file(&path)
                .with_context(|| format!("removing obsolete extension file {}", path.display()))?;
        }
    }

    fs::remove_dir(staging)
        .with_context(|| format!("removing empty extension staging {}", staging.display()))?;
    Ok(())
}

fn activate_staged_extension_entry(source: &Path, destination: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    anyhow::ensure!(
        !metadata.file_type().is_symlink(),
        "extension staging contains unsupported symlink: {}",
        source.display()
    );

    if metadata.is_dir() {
        if destination.exists() && !destination.is_dir() {
            fs::remove_file(destination).with_context(|| {
                format!(
                    "removing file replaced by extension directory {}",
                    destination.display()
                )
            })?;
        }
        activate_staged_extension_directory(source, destination, false)?;
        return Ok(());
    }

    anyhow::ensure!(
        metadata.is_file(),
        "extension staging contains unsupported entry: {}",
        source.display()
    );
    if destination.is_dir() {
        fs::remove_dir_all(destination).with_context(|| {
            format!(
                "removing directory replaced by extension file {}",
                destination.display()
            )
        })?;
    }
    fs::rename(source, destination).with_context(|| {
        format!(
            "atomically replacing extension file {} with {}",
            destination.display(),
            source.display()
        )
    })
}

fn copy_extension_tree(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir(destination)
        .with_context(|| format!("creating extension staging {}", destination.display()))?;

    for entry in fs::read_dir(source)
        .with_context(|| format!("reading extension source {}", source.display()))?
    {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&source_path)?;

        anyhow::ensure!(
            !metadata.file_type().is_symlink(),
            "extension source contains unsupported symlink: {}",
            source_path.display()
        );

        if metadata.is_dir() {
            copy_extension_tree(&source_path, &destination_path)?;
        } else if metadata.is_file() {
            fs::copy(&source_path, &destination_path).with_context(|| {
                format!(
                    "copying extension file {} to {}",
                    source_path.display(),
                    destination_path.display()
                )
            })?;
        } else {
            anyhow::bail!(
                "extension source contains unsupported entry: {}",
                source_path.display()
            );
        }
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
  mirrarium extension install [SOURCE_DIR]
  mirrarium extension status
  mirrarium extension uninstall
  mirrarium native-host install [BROWSER] [MIRRARIUMD_PATH]
  mirrarium native-host status [BROWSER]
  mirrarium native-host uninstall [BROWSER]

EXTENSION:
  SOURCE_DIR defaults to ./extension/dist.
  MIRRARIUM_EXTENSION_SOURCE overrides the source.
  MIRRARIUM_EXTENSION_DIR overrides the stable installed-extension directory.
  MIRRARIUM_EXTENSION_STATE_FILE overrides the public build-state file.
  Updates preserve the stable directory. If extension status reports
  reload_required=true, reload Mirrarium once in edge://extensions or restart
  the browser; the extension does not attempt to hot-reload itself.

NATIVE HOST:
  BROWSER defaults to edge.
  MIRRARIUM_BROWSER_USER_DATA_DIR overrides the browser user-data root.

DATA ROOT:
  MIRRARIUM_DATA_DIR, then XDG_DATA_HOME/mirrarium,
  then ~/.local/share/mirrarium

PRIVATE KEY:
  MIRRARIUM_PRIVATE_KEY_FILE overrides the key path.
  Normal application data roots use XDG_CONFIG_HOME/mirrarium/private.key or
  ~/.config/mirrarium/private.key; isolated roots used by tests/embeddings keep
  a self-contained .keys/private.key."
    );
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_install_atomically_replaces_previous_tree() {
        let directory = tempfile::tempdir().unwrap();
        let source_one = directory.path().join("source-one");
        let source_two = directory.path().join("source-two");
        let destination = directory.path().join("installed");

        for (source, version, marker) in [
            (&source_one, "0.1.0", "first"),
            (&source_two, "0.2.0", "second"),
        ] {
            fs::create_dir_all(source).unwrap();
            fs::write(
                source.join("manifest.json"),
                serde_json::to_vec(&serde_json::json!({
                    "manifest_version": 3,
                    "name": "Mirrarium",
                    "version": version,
                    "version_name": format!("{version}+fixture"),
                    "key": "fixture-key"
                }))
                .unwrap(),
            )
            .unwrap();
            fs::write(source.join("background.js"), format!("// {marker}")).unwrap();
        }
        fs::write(source_one.join("old-only.txt"), b"old").unwrap();

        let first = install_extension(&source_one, &destination).unwrap();
        assert_eq!(first["version"], "0.1.0");
        assert!(destination.join("old-only.txt").is_file());

        let second = install_extension(&source_two, &destination).unwrap();
        assert_eq!(second["version"], "0.2.0");
        assert!(!destination.join("old-only.txt").exists());
        assert_eq!(
            fs::read_to_string(destination.join("background.js")).unwrap(),
            "// second"
        );
    }

    #[cfg(unix)]
    #[test]
    fn extension_install_rejects_symlinked_source_entries() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        let destination = directory.path().join("installed");
        fs::create_dir_all(&source).unwrap();
        fs::write(
            source.join("manifest.json"),
            serde_json::to_vec(&serde_json::json!({
                "manifest_version": 3,
                "name": "Mirrarium",
                "version": "0.1.0",
                "version_name": "0.1.0+fixture",
                "key": "fixture-key"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(source.join("background.js"), b"// fixture").unwrap();
        fs::write(directory.path().join("outside.txt"), b"outside").unwrap();
        symlink(
            directory.path().join("outside.txt"),
            source.join("linked.txt"),
        )
        .unwrap();

        let error = install_extension(&source, &destination)
            .err()
            .expect("symlinked extension source should be rejected");
        assert!(error.to_string().contains("unsupported symlink"));
        assert!(!destination.exists());
    }

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
