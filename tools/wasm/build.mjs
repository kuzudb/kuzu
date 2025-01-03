import * as esbuild from 'esbuild'
import { execSync } from "child_process";
import path from 'path'
import os from 'os'

const SRC_PATH = path.resolve("..", "..");
const THREADS = os.cpus().length;

console.log(`Using ${THREADS} threads to build Kùzu.`);
// console.log('Cleaning up...')
// execSync("npm run clean", { stdio: "inherit" });
console.log('Building single-threaded version of Kùzu WebAssembly module...')
execSync(`make wasm NUM_THREADS=${THREADS} SINGLE_THREAD=true`, {
  cwd: SRC_PATH,
  stdio: "inherit",
});

console.log('Creating esbuild bundle...')
await esbuild.build({
    entryPoints: ['./build/sync/index.js', './build/index.js', 'build/worker.js'],
    bundle: true,
    format: 'esm',
    external: ['fs', 'path', 'ws', 'crypto'],
    outdir: 'dist',
  logLevel: 'info',
  define: {
      "importMeta": "import.meta"
    }
});
