use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use anyhow::{Context, Result};
use serde_json::Value;

/// A script-free representation of the selected ChatGPT conversation branch.
/// Treat all content as untrusted text: never insert response markup directly.
pub fn render_offline_html(
    response: &Value,
    source_url: &str,
    captured_at_ms: u64,
) -> (String, usize) {
    let title = response
        .get("title")
        .and_then(Value::as_str)
        .filter(|title| !title.is_empty())
        .unwrap_or("Archived conversation");
    let messages = selected_messages(response);
    let mut html = String::with_capacity(4096);
    html.push_str("<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">");
    html.push_str("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">");
    html.push_str("<meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; script-src 'none'; connect-src 'none'; img-src 'none'; object-src 'none'; frame-src 'none'; form-action 'none'; base-uri 'none'; style-src 'unsafe-inline'\">");
    html.push_str("<title>");
    html.push_str(&escape_html(title));
    html.push_str(" — Mirrarium offline</title>");
    html.push_str("<style>");
    html.push_str("html{color-scheme:dark light;font-family:system-ui,Arial,sans-serif}body{margin:0 auto;max-width:880px;padding:clamp(16px,4vw,48px);background:Canvas;color:CanvasText}");
    html.push_str("header{margin-bottom:32px;border-bottom:1px solid GrayText;padding-bottom:18px}h1{font-size:1.65rem;line-height:1.3;overflow-wrap:anywhere}");
    html.push_str(".note,.metadata{color:GrayText;font-size:.87rem;line-height:1.6}.metadata{overflow-wrap:anywhere}");
    html.push_str("article{margin:20px 0;padding:18px 22px;border:1px solid GrayText;border-radius:12px}");
    html.push_str("article.user{border-inline-start:4px solid #629aef}article.assistant{border-inline-start:4px solid #48a28c}");
    html.push_str(".role{font-size:.8rem;font-weight:750;letter-spacing:.04em;text-transform:uppercase;margin-bottom:12px}");
    html.push_str(".content{white-space:pre-wrap;overflow-wrap:anywhere;line-height:1.6}");
    html.push_str("</style></head><body><main><header><p class=\"note\">MIRRARIUM · OFFLINE SNAPSHOT</p><h1>");
    html.push_str(&escape_html(title));
    html.push_str("</h1><p class=\"metadata\">Source: ");
    html.push_str(&escape_html(source_url));
    html.push_str("</p><p class=\"metadata\">Captured at Unix milliseconds: ");
    html.push_str(&captured_at_ms.to_string());
    html.push_str("</p><p class=\"note\">Historical local copy. Freshness and current account access have not been verified. No website connection is used.</p></header>");
    if messages.is_empty() {
        html.push_str("<p class=\"note\">No supported text messages were found in this captured response.</p>");
    }
    for (role, body) in &messages {
        let class = match role.as_str() {
            "user" => "user",
            "assistant" => "assistant",
            _ => "other",
        };
        html.push_str("<article class=\"");
        html.push_str(class);
        html.push_str("\"><div class=\"role\">");
        html.push_str(&escape_html(role));
        html.push_str("</div><div class=\"content\">");
        html.push_str(&escape_html(body));
        html.push_str("</div></article>");
    }
    html.push_str("</main></body></html>\n");
    (html, messages.len())
}

fn selected_messages(response: &Value) -> Vec<(String, String)> {
    if let (Some(mapping), Some(mut current)) = (
        response.get("mapping").and_then(Value::as_object),
        response.get("current_node").and_then(Value::as_str),
    ) {
        let mut reversed = Vec::new();
        let mut visited = BTreeSet::new();
        // Stop on malformed cycles or missing parents; never display sibling
        // branches that were not selected by the captured current_node.
        while visited.insert(current.to_owned()) && visited.len() <= 100_000 {
            let Some(node) = mapping.get(current) else { break; };
            if let Some(message) = node.get("message") {
                if let Some(item) = extract_message(message) {
                    reversed.push(item);
                }
            }
            let Some(parent) = node.get("parent").and_then(Value::as_str) else {
                break;
            };
            current = parent;
        }
        reversed.reverse();
        return reversed;
    }

    response
        .get("messages")
        .and_then(Value::as_array)
        .map(|messages| messages.iter().filter_map(extract_message).collect())
        .unwrap_or_default()
}

fn extract_message(message: &Value) -> Option<(String, String)> {
    let role = message
        .pointer("/author/role")
        .or_else(|| message.get("role"))
        .and_then(Value::as_str)?
        .to_owned();
    let content = message.get("content")?;
    let text = if let Some(value) = content.as_str() {
        value.to_owned()
    } else if let Some(parts) = content.get("parts").and_then(Value::as_array) {
        parts
            .iter()
            .filter_map(|part| {
                part.as_str()
                    .or_else(|| part.get("text").and_then(Value::as_str))
            })
            .collect::<Vec<_>>()
            .join("\n\n")
    } else {
        content.get("text").and_then(Value::as_str)?.to_owned()
    };
    if text.is_empty() {
        None
    } else {
        Some((role, text))
    }
}

fn escape_html(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for character in input.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

/// Write a plaintext private snapshot only to a *new* user-selected file.
/// Never truncate an existing file or follow an existing output symlink.
pub fn write_private_html(path: &Path, html: &str) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("creating new private offline page {}", path.display()))?;
    let saved = file
        .write_all(html.as_bytes())
        .and_then(|()| file.sync_all());
    if let Err(error) = saved {
        drop(file);
        let _ = fs::remove_file(path);
        return Err(error).with_context(|| format!("writing private offline page {}", path.display()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn selected_branch_escapes_markup_and_omits_discarded_siblings() {
        let response = json!({
            "title": "<script>alert('title')</script>",
            "current_node": "assistant-b",
            "mapping": {
                "root": {"parent": null, "message": null},
                "user": {"parent": "root", "message": {
                    "author": {"role":"user"}, "content":{"parts":["Question <img src=x onerror=alert(1)>"]}
                }},
                "assistant-a": {"parent": "user", "message": {
                    "author":{"role":"assistant"}, "content":{"parts":["discarded branch"]}
                }},
                "assistant-b": {"parent": "user", "message": {
                    "author":{"role":"assistant"}, "content":{"parts":[{"text":"Answer & details"}]}
                }}
            }
        });
        let (html, count) = render_offline_html(&response, "https://chatgpt.com/c/abc?bad=<x>", 123);
        assert_eq!(count, 2);
        assert!(html.contains("&lt;script&gt;alert(&#39;title&#39;)&lt;/script&gt;"));
        assert!(html.contains("Question &lt;img src=x onerror=alert(1)&gt;"));
        assert!(html.contains("Answer &amp; details"));
        assert!(!html.contains("discarded branch"));
        assert!(!html.contains("<script>"));
        assert!(html.contains("script-src 'none'"));
        assert!(html.find("Question").unwrap() < html.find("Answer").unwrap());
    }

    #[test]
    fn simple_message_array_and_empty_messages_are_supported() {
        let (html, count) = render_offline_html(
            &json!({"title":"Example","messages":[
                {"role":"user","content":"hello"},
                {"role":"assistant","content":{"parts":["world"]}}
            ]}),
            "https://chatgpt.com/c/example", 42,
        );
        assert_eq!(count, 2);
        assert!(html.contains("hello"));
        assert!(html.contains("world"));
        let (empty, count) = render_offline_html(&json!({}), "x", 0);
        assert_eq!(count, 0);
        assert!(empty.contains("No supported text messages"));
    }

    #[test]
    fn private_html_export_is_exclusive_and_owner_only() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("offline.html");
        write_private_html(&path, "<html>private snapshot</html>").unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "<html>private snapshot</html>");
        assert!(write_private_html(&path, "replacement").is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), "<html>private snapshot</html>");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        }
    }
}
