const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");

const SRC_PATH = path.resolve(__dirname, "../..");
const THREADS = require("os").cpus().length;

console.log(`Using ${THREADS} threads to build Kuzu.`);

execSync("npm run clean", { stdio: "inherit" });
execSync(`make nodejs NUM_THREADS=${THREADS}`, {
  cwd: SRC_PATH,
  stdio: "inherit",
});

// Copy TypeScript definitions and ES modules
console.log("Copying TypeScript definitions and ES modules to build directory...");
const jsSourceDir = path.join(__dirname, "src_js");
const buildDir = path.join(__dirname, "build");

// Ensure build directory exists
if (!fs.existsSync(buildDir)) {
  fs.mkdirSync(buildDir);
}

// Copy all JS files, including .d.ts and .mjs
const jsFiles = fs.readdirSync(jsSourceDir).filter((file) => {
  return file.endsWith(".js") || file.endsWith(".mjs") || file.endsWith(".d.ts");
});

console.log("Files to copy:");
for (const file of jsFiles) {
  console.log("  " + file);
  fs.copyFileSync(path.join(jsSourceDir, file), path.join(buildDir, file));
}

console.log("TypeScript definitions and ES modules copied successfully.");