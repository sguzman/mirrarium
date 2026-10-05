import http from "node:http";

const host = "127.0.0.1";
const port = 43117;

const server = http.createServer((request, response) => {
  if (request.url === "/health") {
    response.writeHead(200, { "content-type": "text/plain" });
    response.end("ok");
    return;
  }

  response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
  response.end("<!doctype html><title>Mirrarium fixture</title><h1>fixture</h1>");
});

server.listen(port, host, () => {
  process.stdout.write(`fixture server listening on http://${host}:${port}\n`);
});
