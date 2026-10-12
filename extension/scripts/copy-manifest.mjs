import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";

const dist = new URL("../dist/", import.meta.url);
const sourceManifest = new URL("../manifest.json", import.meta.url);
const background = new URL("background.js", dist);
const sourcePopup = new URL("../popup.html", import.meta.url);

await mkdir(dist, { recursive: true });

const manifest = JSON.parse(await readFile(sourceManifest, "utf8"));
const backgroundBytes = await readFile(background);
const buildHash = createHash("sha256")
  .update(backgroundBytes)
  .update("\0")
  .update(JSON.stringify(manifest))
  .digest("hex")
  .slice(0, 16);

manifest.version_name = `${manifest.version}+${buildHash}`;

await writeFile(new URL("popup.html", dist), await readFile(sourcePopup));
await writeFile(
  new URL("manifest.json", dist),
  JSON.stringify(manifest, null, 2) + "\n",
);
