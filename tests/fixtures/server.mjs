import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
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
let staticCssHits = 0;
let staticJsHits = 0;
let staticImageHits = 0;
let staticFontHits = 0;
let staticStressHits = 0;
let cdnJsHits = 0;
let privateConversationHits = 0;
let privateConversationConditional304s = 0;
let privateConversationVersion = 1;
let privateQueryHits = 0;
let privateQueryConditional304s = 0;
let privateDocumentHits = 0;
let privateDocumentConditional304s = 0;

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
  "subjectAltName=DNS:chatgpt.com,DNS:cdn.oaistatic.com",
], { stdio: "ignore" });

const stressScriptCount = 16;
const stressScriptTags = Array.from(
  { length: stressScriptCount },
  (_, index) => `<script src="/_next/static/stress-${index}.js"></script>`,
).join("\n");

function encodeWebSocketTextFrame(text) {
  const payload = Buffer.from(text, "utf8");
  if (payload.length >= 126) {
    throw new Error("fixture WebSocket payload unexpectedly large");
  }
  return Buffer.concat([Buffer.from([0x81, payload.length]), payload]);
}

function encodeWebSocketBinaryFrame(payload) {
  if (payload.length >= 126) {
    throw new Error("fixture WebSocket binary payload unexpectedly large");
  }
  return Buffer.concat([Buffer.from([0x82, payload.length]), payload]);
}

const page = `<!doctype html>
<meta charset="utf-8">
<title>Mirrarium fixture</title>
<link rel="stylesheet" href="/_next/static/app.css">
<h1>fixture</h1>
<img alt="fixture pixel" src="/_next/static/pixel.svg">
<script src="/_next/static/app.js"></script>
${stressScriptTags}
<script src="https://cdn.oaistatic.com:43117/assets/cdn-app.js"></script>
<script>
const requestMessage = ["hello", "from", "request", "body"].join(" ");
const requestSecret = ["fixture", "secret", "token"].join("-");
const redirectPostSecret = ["fixture", "redirect", "post", "secret"].join("-");
const abortPostSecret = ["fixture", "abort", "post", "secret"].join("-");
Promise.all([
  fetch("/backend-api/conversation/test").then((response) => response.json()),
  fetch("/backend-api/conversations?offset=0&limit=2").then((response) =>
    response.json(),
  ),
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
  fetch("/backend-api/conversation/stream-tail")
    .then((response) => response.json())
    .then(
      () =>
        new Promise((resolve) => setTimeout(resolve, 50)),
    )
    .then(() =>
      fetch("/backend-api/conversation/stream-tail/events").then((response) =>
        response.text(),
      ),
    ),
  fetch("/backend-api/redirect-start").then((response) => response.text()),
  fetch("/backend-api/redirect-post-start", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      message: "preserve this redirect body",
      access_token: redirectPostSecret,
    }),
  }).then((response) => response.json()),
  fetch("/backend-api/redirect-see-other-start", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      message: "drop this body after 303",
      access_token: redirectPostSecret,
    }),
  }).then((response) => response.json()),
  fetch("/backend-api/abort-fixture", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      message: "preserve request evidence on failed response",
      access_token: abortPostSecret,
    }),
  })
    .then((response) => response.text())
    .catch(() => null),
  fetch("/backend-api/no-content-fixture").then((response) => response.text()),
  fetch("/backend-api/head-fixture", { method: "HEAD" }).then((response) =>
    response.text(),
  ),
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

const replayProbe = `<!doctype html>
<meta charset="utf-8">
<title>Mirrarium replay probe</title>
<link rel="stylesheet" href="/_next/static/app.css">
<img alt="replay pixel" src="/_next/static/pixel.svg">
<script src="/_next/static/app.js"></script>
<script src="https://cdn.oaistatic.com:43117/assets/cdn-app.js"></script>
<h1>replay probe</h1>`;

const replayStressProbe = `<!doctype html>
<meta charset="utf-8">
<title>Mirrarium replay stress probe</title>
<h1>replay stress probe</h1>
${stressScriptTags}
<script>
Promise.all(
  Array.from({ length: 32 }, (_, index) =>
    fetch("/backend-api/stress-write/" + index, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        message: "stress write " + index,
        access_token: "fixture-stress-secret",
      }),
    }).then((response) => response.json()),
  ),
).then(() => {
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

    if (request.url === "/replay-probe") {
      response.writeHead(200, {
        "content-type": "text/html; charset=utf-8",
        "cache-control": "no-store",
      });
      response.end(replayProbe);
      return;
    }

    if (request.url === "/replay-stress-probe") {
      response.writeHead(200, {
        "content-type": "text/html; charset=utf-8",
        "cache-control": "no-store",
      });
      response.end(replayStressProbe);
      return;
    }

    if (/^\/_next\/static\/stress-\d+\.js$/.test(request.url ?? "")) {
      staticStressHits += 1;
      response.writeHead(200, {
        "content-type": "application/javascript",
        "cache-control": "public, max-age=31536000, immutable",
      });
      response.end(
        "globalThis.__mirrariumStressLoaded = (globalThis.__mirrariumStressLoaded ?? 0) + 1;",
      );
      return;
    }

    if (request.url === "/_next/static/app.css") {
      staticCssHits += 1;
      response.writeHead(200, {
        "content-type": "text/css",
        "cache-control": "public, max-age=31536000, immutable",
      });
      response.end(
        '@font-face { font-family: "MirrariumFixture"; src: url("/_next/static/fixture.woff2") format("woff2"); } body { font-family: "MirrariumFixture", sans-serif; }',
      );
      return;
    }

    if (request.url === "/_next/static/fixture.woff2") {
      staticFontHits += 1;
      response.writeHead(200, {
        "content-type": "font/woff2",
        "cache-control": "public, max-age=31536000, immutable",
      });
      response.end(Buffer.from("fixture-woff2-placeholder"));
      return;
    }

    if (request.url === "/_next/static/pixel.svg") {
      staticImageHits += 1;
      response.writeHead(200, {
        "content-type": "image/svg+xml",
        "cache-control": "public, max-age=31536000, immutable",
      });
      response.end(
        '<svg xmlns="http://www.w3.org/2000/svg" width="2" height="2"><rect width="2" height="2"/></svg>',
      );
      return;
    }

    if (
      request.url === "/assets/cdn-app.js" &&
      request.headers.host?.startsWith("cdn.oaistatic.com:")
    ) {
      cdnJsHits += 1;
      response.writeHead(200, {
        "content-type": "application/javascript",
        "cache-control": "public, max-age=31536000, immutable",
      });
      response.end("globalThis.__mirrariumCdnFixtureLoaded = true;");
      return;
    }

    if (request.url === "/_next/static/app.js") {
      staticJsHits += 1;
      response.writeHead(200, {
        "content-type": "application/javascript",
        "cache-control": "public, max-age=31536000, immutable",
      });
      response.end("globalThis.__mirrariumFixtureLoaded = true;");
      return;
    }

    if (request.url === "/backend-api/conversation/test") {
      privateConversationHits += 1;
      const etag = `"fixture-conversation-v${privateConversationVersion}"`;
      if (request.headers["if-none-match"] === etag) {
        privateConversationConditional304s += 1;
        response.writeHead(304, {
          etag,
          "cache-control": "private, max-age=0, must-revalidate",
          "x-mirrarium-origin-304": "preserved",
        });
        response.end();
        return;
      }

      response.writeHead(200, {
        "content-type": "application/json",
        etag,
        "cache-control": "private, max-age=0, must-revalidate",
      });
      response.end(JSON.stringify({
        id: "fixture-conversation",
        title: "Private fixture",
        version: privateConversationVersion,
        messages: [{ role: "user", content: "private corpus material" }],
      }));
      return;
    }

    if (request.url === "/backend-api/conversations?offset=0&limit=2") {
      privateQueryHits += 1;
      const etag = "\"fixture-conversations-page-v1\"";
      if (request.headers["if-none-match"] === etag) {
        privateQueryConditional304s += 1;
        response.writeHead(304, {
          etag,
          "cache-control": "private, max-age=0, must-revalidate",
          "x-mirrarium-origin-304": "preserved",
        });
        response.end();
        return;
      }
      response.writeHead(200, {
        "content-type": "application/json",
        etag,
        "cache-control": "private, max-age=0, must-revalidate",
      });
      response.end(JSON.stringify({
        items: ["conversation-a", "conversation-b"],
        offset: 0,
        limit: 2,
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

    if (
      request.url?.startsWith("/backend-api/stress-write/") &&
      request.method === "POST"
    ) {
      request.resume();
      request.on("end", () => {
        response.writeHead(200, {
          "content-type": "application/json",
          "cache-control": "no-store",
        });
        response.end(JSON.stringify({ ok: true }));
      });
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

    if (request.url === "/backend-api/abort-fixture" && request.method === "POST") {
      request.resume();
      request.on("end", () => {
        response.writeHead(200, {
          "content-type": "application/json",
          "content-length": "4096",
          "cache-control": "no-store",
          "x-mirrarium-abort": "fixture",
        });
        response.flushHeaders();
        setTimeout(() => {
          response.write('{"partial":"response');
          response.socket?.destroy();
        }, 50);
      });
      return;
    }

    if (request.url === "/backend-api/no-content-fixture") {
      response.writeHead(204, {
        "x-mirrarium-no-body": "204",
      });
      response.end();
      return;
    }

    if (request.url === "/backend-api/head-fixture" && request.method === "HEAD") {
      response.writeHead(200, {
        "content-type": "text/plain",
        "content-length": "123",
        "x-mirrarium-no-body": "head",
      });
      response.end();
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

    if (
      request.url === "/backend-api/redirect-post-start" &&
      request.method === "POST"
    ) {
      request.resume();
      request.on("end", () => {
        response.writeHead(307, {
          location:
            "/backend-api/redirect-post-final?token=fixture-redirect-307-secret",
        });
        response.end();
      });
      return;
    }

    if (
      request.url?.startsWith("/backend-api/redirect-post-final") &&
      request.method === "POST"
    ) {
      let body = "";
      request.setEncoding("utf8");
      request.on("data", (chunk) => {
        body += chunk;
      });
      request.on("end", () => {
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify({ ok: true, received_bytes: body.length }));
      });
      return;
    }

    if (
      request.url === "/backend-api/redirect-see-other-start" &&
      request.method === "POST"
    ) {
      request.resume();
      request.on("end", () => {
        response.writeHead(303, {
          location:
            "/backend-api/redirect-see-other-final?token=fixture-redirect-303-secret",
        });
        response.end();
      });
      return;
    }

    if (
      request.url?.startsWith("/backend-api/redirect-see-other-final") &&
      request.method === "GET"
    ) {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ ok: true, method: request.method }));
      return;
    }

    if (request.url === "/backend-api/conversation/stream-tail") {
      response.writeHead(200, {
        "content-type": "application/json",
        etag: "\"fixture-stream-tail-v1\"",
      });
      response.end(JSON.stringify({
        id: "fixture-stream-tail",
        title: "Stream tail fixture",
        current_node: "stream-tail-user",
        mapping: {
          root: {
            parent: null,
            children: ["stream-tail-user"],
            message: null,
          },
          "stream-tail-user": {
            parent: "root",
            children: [],
            message: {
              id: "stream-tail-user",
              author: { role: "user" },
              content: { parts: ["question"] },
            },
          },
        },
      }));
      return;
    }

    if (request.url === "/backend-api/conversation/stream-tail/events") {
      response.writeHead(200, {
        "content-type": "text/event-stream; charset=utf-8",
        "cache-control": "no-cache",
      });
      response.write("event: message\n");
      response.write(
        'data: {"conversation_id":"fixture-stream-tail","parent_message_id":"stream-tail-user","message":{"id":"stream-tail-assistant","author":{"role":"assistant"},"content":{"parts":["hello"]}}}\n\n',
      );
      response.write("event: message\n");
      response.write(
        'data: {"conversation_id":"fixture-stream-tail","parent_message_id":"stream-tail-user","message":{"id":"stream-tail-assistant","author":{"role":"assistant"},"content":{"parts":["hello world"]}}}\n\n',
      );
      response.end("data: [DONE]\n\n");
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

    if (request.url === "/") {
      privateDocumentHits += 1;
      const etag = "\"fixture-document-v1\"";
      if (request.headers["if-none-match"] === etag) {
        privateDocumentConditional304s += 1;
        response.writeHead(304, {
          etag,
          "cache-control": "private, max-age=0, must-revalidate",
          "x-mirrarium-origin-304": "preserved",
        });
        response.end();
        return;
      }
      response.writeHead(200, {
        "content-type": "text/html; charset=utf-8",
        etag,
        "cache-control": "private, max-age=0, must-revalidate",
      });
      response.end(page);
      return;
    }

    response.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
    });
    response.end(page);
  },
);

fixtureServer.on("upgrade", (request, socket) => {
  if (!request.url?.startsWith("/backend-api/ws-fixture")) {
    socket.destroy();
    return;
  }

  const key = request.headers["sec-websocket-key"];
  if (typeof key !== "string") {
    socket.destroy();
    return;
  }

  const accept = createHash("sha1")
    .update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
    .digest("base64");
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
      "Upgrade: websocket\r\n" +
      "Connection: Upgrade\r\n" +
      `Sec-WebSocket-Accept: ${accept}\r\n` +
      "X-Mirrarium-WebSocket: fixture\r\n" +
      "\r\n",
  );

  let replied = false;
  socket.on("data", () => {
    if (replied) {
      socket.end();
      return;
    }
    replied = true;
    socket.write(
      Buffer.concat([
        encodeWebSocketTextFrame(
          JSON.stringify({
            message: "server websocket fixture",
            access_token: "fixture-ws-server-secret",
          }),
        ),
        encodeWebSocketTextFrame("plain websocket fixture"),
        encodeWebSocketBinaryFrame(Buffer.from([1, 2, 3, 4])),
      ]),
    );
  });
});

const healthServer = http.createServer((request, response) => {
  if (request.url === "/private-bump") {
    privateConversationVersion += 1;
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ version: privateConversationVersion }));
    return;
  }

  if (request.url === "/private-document-counts") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      total: privateDocumentHits,
      conditional_304: privateDocumentConditional304s,
    }));
    return;
  }

  if (request.url === "/private-query-counts") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      total: privateQueryHits,
      conditional_304: privateQueryConditional304s,
    }));
    return;
  }

  if (request.url === "/private-revalidation-counts") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      total: privateConversationHits,
      conditional_304: privateConversationConditional304s,
    }));
    return;
  }

  if (request.url === "/replay-counts") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      css: staticCssHits,
      js: staticJsHits,
      image: staticImageHits,
      font: staticFontHits,
      stress_js: staticStressHits,
      cdn_js: cdnJsHits,
    }));
    return;
  }

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
