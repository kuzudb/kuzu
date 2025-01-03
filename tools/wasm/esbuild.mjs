import * as esbuild from 'esbuild'

await esbuild.build({
    entryPoints: ['./build/sync/index.js', './build/index.js', 'build/worker.js'],
    bundle: true,
    format: 'esm',
    external: ['fs', 'path', 'ws', 'crypto'],
    outdir: 'dist',
    logLevel: 'info',
});
