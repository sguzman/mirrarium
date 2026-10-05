import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import http from "node:http";
import https from "node:https";
import { tmpdir } from "node:os";
import { join } from "node:path";

const httpsPort = 43117;
const healthPort = 43118;
const certificateDirectory = mkdtempSync(join(tmpdir(), "mirrarium-cert-"));
const keyPath = join(certificateDirectory, "key.pem");
const certPath = join(certificateDirectory, "cert.pem");

execFileSync("openssl", [
  "req",
  "-x509",
  "-newkey",
  "rsa:2048",
  "-nodes",
  "-keyout",
  keyPath,
  "-out",
  certPath,
  "-days",
  "1",
  "-subj",
  "/CN=chatgpt.com",
  "-addext",
  "subjectAltName=DNS:chatgpt.com",
], { stdio: "ignore" });

const page = `<!doctype html>
<meta charset="utf-8">
<title>Mirrarium fixture</title>
<link rel="stylesheet" href="/_next/static/app.css">
<h1>fixture</h1>
<script src="/_next/static/app.js"></script>
<script>
const requestMessage = ["hello", "from", "request", "body"].join(" ");
const requestSecret = ["fixture", "secret", "token"].join("-");
Promise.all([
  fetch("/backend-api/conversation/test").then((response) => response.json()),
  fetch("/backend-api/conversation/branch-fixture").then((response) => response.json()),
  fetch("/backend-api/conversation/attachment-fixture")
    .then((response) => response.json())
    .then((conversation) =>
      fetch(
        conversation.mapping["attachment-message"].message.content.attachments[0].download_url,
      ).then((response) => response.text()),
    ),
  fetch("/backend-api/repeat/a").then((response) => response.text()),
  fetch("/backend-api/repeat/b").then((response) => response.text()),
  fetch("/backend-api/conversation/post", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "authorization": "Bearer fixture-header-secret",
      "x-mirrarium-fixture": "preserve-me",
    },
    body: JSON.stringify({
      message: requestMessage,
      access_token: requestSecret,
    }),
  }).then((response) => response.json()),
  fetch("/backend-api/conversation/stream").then((response) => response.text()),
  fetch("/backend-api/redirect-start").then((response) => response.text()),
  fetch("/backend-api/upload-fixture", {
    method: "POST",
    body: (() => {
      const form = new FormData();
      form.append("note", "upload metadata only");
      form.append(
        "file",
        new Blob(["fixture-file-bytes"], { type: "text/plain" }),
        "fixture.txt",
      );
      return form;
    })(),
  }).then((response) => response.json())
]).then(() => {
  document.body.dataset.ready = "yes";
});
</script>`;

const fixtureServer = https.createServer(
  {
    key: readFileSync(keyPath),
    cert: readFileSync(certPath),
  },
  (request, response) => {
    if (request.url === "/warmup") {
      response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      response.end("<!doctype html><title>warmup</title>");
      return;
    }

    if (request.url === "/_next/static/app.css") {
      response.writeHead(200, {
        "content-type": "text/css",
        "cache-control": "public, max-age=31536000, immutable",
      });
      response.end("body { font-family: sans-serif; }");
      return;
    }

    if (request.url === "/_next/static/app.js") {
      response.writeHead(200, {
        "content-type": "application/javascript",
        "cache-control": "public, max-age=31536000, immutable",
      });
      response.end("globalThis.__mirrariumFixtureLoaded = true;");
      return;
    }

    if (request.url === "/backend-api/conversation/test") {
      response.writeHead(200, {
        "content-type": "application/json",
        etag: "\"fixture-conversation-v1\"",
      });
      response.end(JSON.stringify({
        id: "fixture-conversation",
        title: "Private fixture",
        messages: [{ role: "user", content: "private corpus material" }],
      }));
      return;
    }

    if (request.url === "/backend-api/conversation/branch-fixture") {
      response.writeHead(200, {
        "content-type": "application/json",
        etag: "\"fixture-branched-v1\"",
      });
      response.end(JSON.stringify({
        id: "fixture-branched",
        title: "Branched fixture",
        current_node: "assistant-b",
        mapping: {
          root: {
            parent: null,
            children: ["user-1"],
            message: null,
          },
          "user-1": {
            parent: "root",
            children: ["assistant-a", "assistant-b"],
            message: {
              id: "user-1",
              author: { role: "user" },
              content: { parts: ["question"] },
            },
          },
          "assistant-a": {
            parent: "user-1",
            children: [],
            message: {
              id: "assistant-a",
              author: { role: "assistant" },
              content: { parts: ["discarded branch"] },
            },
          },
          "assistant-b": {
            parent: "user-1",
            children: [],
            message: {
              id: "assistant-b",
              author: { role: "assistant" },
              content: { parts: ["chosen branch"] },
            },
          },
        },
      }));
      return;
    }

    if (request.url === "/backend-api/conversation/attachment-fixture") {
      response.writeHead(200, {
        "content-type": "application/json",
        etag: "\"fixture-attachment-v1\"",
      });
      response.end(JSON.stringify({
        id: "fixture-attachment-conversation",
        title: "Attachment fixture",
        current_node: "attachment-message",
        mapping: {
          root: {
            parent: null,
            children: ["attachment-message"],
            message: null,
          },
          "attachment-message": {
            parent: "root",
            children: [],
            message: {
              id: "attachment-message",
              author: { role: "user" },
              content: {
                parts: ["attachment"],
                attachments: [{
                  file_id: "file-123",
                  filename: "fixture-attachment.txt",
                  mime_type: "text/plain",
                  size_bytes: 24,
                  download_url:
                    "/backend-api/files/file-123/download?token=fixture-download-secret&keep=yes",
                }],
              },
            },
          },
        },
      }));
      return;
    }

    if (request.url?.startsWith("/backend-api/files/file-123/download")) {
      response.writeHead(200, {
        "content-type": "text/plain",
        "content-disposition": 'attachment; filename="fixture-attachment.txt"',
      });
      response.end("fixture attachment bytes");
      return;
    }

    if (request.url === "/backend-api/upload-fixture" && request.method === "POST") {
      request.resume();
      request.on("end", () => {
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify({ ok: true }));
      });
      return;
    }

    if (request.url === "/backend-api/redirect-start") {
      response.writeHead(302, {
        location: "/backend-api/redirect-final?token=fixture-redirect-secret",
      });
      response.end();
      return;
    }

    if (request.url?.startsWith("/backend-api/redirect-final")) {
      response.writeHead(200, { "content-type": "text/plain" });
      response.end("redirect complete");
      return;
    }

    if (request.url === "/backend-api/conversation/stream") {
      response.writeHead(200, {
        "content-type": "text/event-stream; charset=utf-8",
        "cache-control": "no-cache",
      });
      response.write("event: message\n");
      response.write('data: {"conversation_id":"fixture-stream","delta":"hello"}\n\n');
      response.write("event: message\n");
      response.write('data: {"conversation_id":"fixture-stream","delta":" world"}\n\n');
      response.end("data: [DONE]\n\n");
      return;
    }

    if (
      request.url === "/backend-api/conversation/post" &&
      request.method === "POST"
    ) {
      let incoming = "";
      request.setEncoding("utf8");
      request.on("data", (chunk) => {
        incoming += chunk;
      });
      request.on("end", () => {
        response.writeHead(200, {
          "content-type": "application/json",
          "x-mirrarium-response": "preserve-me-too",
        });
        response.end(JSON.stringify({ ok: true, received_bytes: incoming.length }));
      });
      return;
    }

    if (
      request.url === "/backend-api/repeat/a" ||
      request.url === "/backend-api/repeat/b"
    ) {
      response.writeHead(200, { "content-type": "text/plain" });
      response.end("identical private bytes");
      return;
    }

    response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    response.end(page);
  },
);

const healthServer = http.createServer((_request, response) => {
  response.writeHead(200, { "content-type": "text/plain" });
  response.end("ok");
});

fixtureServer.listen(httpsPort, "127.0.0.1");
healthServer.listen(healthPort, "127.0.0.1", () => {
  process.stdout.write(
    `Mirrarium fixture listening on https://chatgpt.com:${httpsPort}\n`,
  );
});

function shutdown() {
  fixtureServer.close();
  healthServer.close();
  rmSync(certificateDirectory, { recursive: true, force: true });
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
