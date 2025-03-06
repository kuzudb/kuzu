import * as esbuild from 'esbuild'
import { execSync } from "child_process";
import path from 'path';
import os from 'os';
import fs from 'fs';

const SRC_PATH = path.resolve("..", "..");
const THREADS = os.cpus().length;
const KUZU_VERSION_TEXT = "Kuzu VERSION";
const ES_BUILD_CONFIG = {
  entryPoints: ['./build/sync/index.js', './build/index.js', 'build/kuzu_wasm_worker.js'],
  bundle: true,
  format: 'esm',
  external: ['fs', 'path', 'ws', 'crypto', "worker_threads", "os"],
  outdir: 'package',
  logLevel: 'info',
  define: {
    "importMeta": "import.meta"
  }
};

console.log(`Using ${THREADS} threads to build Kuzu.`);
console.log('Cleaning up...');
execSync("npm run clean", { stdio: "inherit" });
console.log('Building single-threaded version of Kuzu WebAssembly module...')
execSync(`make wasm NUM_THREADS=${THREADS} SINGLE_THREADED=true`, {
  cwd: SRC_PATH,
  stdio: "inherit",
});

console.log('Creating esbuild bundle...');
await esbuild.build(ES_BUILD_CONFIG);

console.log('Cleaning up...');
execSync("npm run clean exclude-package", { stdio: "inherit" });
console.log("Building multi-threaded version of Kuzu WebAssembly module...");
execSync(`make wasm NUM_THREADS=${THREADS} SINGLE_THREADED=false`, {
  cwd: SRC_PATH,
  stdio: "inherit",
});
console.log('Creating esbuild bundle...');
const ES_BUILD_CONFIG_MULTI = JSON.parse(JSON.stringify(ES_BUILD_CONFIG));
ES_BUILD_CONFIG_MULTI.outdir = 'package/multithreaded';
await esbuild.build(ES_BUILD_CONFIG_MULTI);

console.log('Cleaning up...');
execSync("npm run clean exclude-package", { stdio: "inherit" });
console.log("Building Node.js version of Kuzu WebAssembly module...");
execSync(`make wasm NUM_THREADS=${THREADS} SINGLE_THREADED=false WASM_NODEFS=true`, {
  cwd: SRC_PATH,
  stdio: "inherit",
});

console.log('Copying Node.js version to package...');
const srcPath = path.resolve('.', 'build');
const dstPath = path.resolve('.', 'package', 'nodejs');
fs.cpSync(srcPath, dstPath, { recursive: true });


const CMakeListsTxt = await fs.promises.readFile(
  path.join(SRC_PATH, "CMakeLists.txt"),
  { encoding: "utf-8" }
);

console.log('Creating package.json...');
const packageJsonText = await fs.promises.readFile(path.resolve(".", 'package.json'), 'utf-8');
const packageJson = JSON.parse(packageJsonText);
const lines = CMakeListsTxt.split("\n");
for (const line of lines) {
  if (line.includes(KUZU_VERSION_TEXT)) {
    const versionSplit = line.split(" ")[2].trim().split(".");
    let version = versionSplit.slice(0, 3).join(".");
    if (versionSplit.length >= 4) {
      version += "-dev." + versionSplit.slice(3).join(".");
    }
    console.log("Found version string from CMakeLists.txt: " + version);
    packageJson.version = version;
    break;
  }
}
delete packageJson.scripts;
delete packageJson.devDependencies;
await fs.promises.writeFile(path.resolve(".", 'package', 'package.json'), JSON.stringify(packageJson, null, 2), 'utf-8');

console.log('Copying LICENSE...');
await fs.promises.copyFile(path.resolve(SRC_PATH, 'LICENSE'), path.resolve(".", 'package', 'LICENSE'));

console.log('Copying README.md...');
await fs.promises.copyFile(path.resolve('.', 'README.md'), path.resolve(".", 'package', 'README.md'));

console.log('Creating tarball...');
execSync("tar -czf kuzu-wasm.tar.gz package", { cwd: path.resolve("."), stdio: "inherit" });

console.log('All done!');
