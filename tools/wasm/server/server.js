const express = require("express");
const serveIndex = require("serve-index");
const path = require("path");
const app = express();

const fileDir = __dirname;
const filePath = path.join(
  fileDir,
  "..",
  "..",
  "..",
  "build",
  "wasm",
  "tools",
  "wasm"
);

app.use((req, res, next) => {
  res.set("Cross-Origin-Opener-Policy", "same-origin");
  res.set("Cross-Origin-Embedder-Policy", "require-corp");
  next();
});

app.use((req, res, next) => {
  console.log(req.method, '\t', req.path);
  next();
});

app.use(express.static(filePath));
app.use(serveIndex(filePath, { icons: true }));

app.listen(3000, () => {
  console.log("Server is listening on port 3000");
});
