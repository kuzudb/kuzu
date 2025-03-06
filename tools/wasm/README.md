# Kuzu-Wasm
Kuzu-Wasm is the official WebAssembly build of Kuzu in-process property graph database management system. 
Kuzu is an embeddable property graph database management system built for query speed and scalability. 
Please visit [Kuzu website](https://kuzudb.com) for more information. Kuzu-Wasm enables the following:

- Fast, in-browser graph analysis without ever sending data to a server
- Strong data privacy guarantees, as the data never leaves the browser
- Real-time interactive dashboards
- Lightweight, portable solutions that leverage graphs within web applications

## Installation

```bash
npm i kuzu-wasm
```

## Example usage

We provide a simple example to demonstrate how to use Kuzu-Wasm. In this example, we will create a simple graph and run a few simple queries.

We provide three versions of this example: 
- `browser_in_memory`: This example demonstrates how to use Kuzu-Wasm in a web browser with an in-memory filesystem.
- `browser_persistent`: This example demonstrates how to use Kuzu-Wasm in a web browser with a persistent IDBFS filesystem.
- `nodejs`: This example demonstrates how to use Kuzu-Wasm in Node.js.

The example can be found in [the examples directory](https://github.com/kuzudb/kuzu/tree/master/tools/wasm/examples).

## Understanding the package

In this package, three different variants of WebAssembly modules are provided:
- **Default**: This is the default build of the WebAssembly module. It does not support multi-threading and uses Emscripten's default filesystem. This build has the smallest size and works in both Node.js and browser environments. It has the best compatibility and does not require cross-origin isolation. However, the performance maybe limited due to the lack of multithreading support. This build is located at the root level of the package.
- **Multi-threaded**: This build supports multi-threading and uses Emscripten's default filesystem. This build has a larger size compared to the default build and only requires [cross-origin isolation](https://web.dev/articles/cross-origin-isolation-guide) in the browser environment. This build is located in the `multithreaded` directory.
- **Node.js**: This build is optimized for Node.js and uses Node.js's filesystem instead of Emscripten's default filesystem (`NODEFS` flag is enabled). This build also supports multi-threading. It is distributed as a CommonJS module rather than an ES module to maximize compatibility. This build is located in the `nodejs` directory. Note that this build only works in Node.js and does not work in the browser environment.

In each variant, there are two different versions of the WebAssembly module:
- **Async**: This version of the module is the default version and each function call returns a Promise. This version dispatches all the function calls to the WebAssembly module to a Web Worker or Node.js worker thread to prevent blocking the main thread. However, this version may have a slight overhead due to the serialization and deserialization of the data required by the worker threads. This version is located at the root level of each variant (e.g., `kuzu-wasm`, `kuzu-wasm/multithreaded`, `kuzu-wasm/nodejs`).
- **Sync**: This version of the module is synchronous and does not require any callbacks (other than the module initialization). This version is good for scripting / CLI / prototyping purposes but is not recommended to be used in GUI applications or web servers because it may block the main thread and cause unexpected freezes. This alternative version is located in the `sync` directory of each variant (e.g., `kuzu-wasm/sync`, `kuzu-wasm/multithreaded/sync`, `kuzu-wasm/nodejs/sync`).

Note that you cannot mix and match the variants and versions. For example, a `Database` object created with the default variant cannot be passed to a function in the multithreaded variant. Similarly, a `Database` object created with the async version cannot be passed to a function in the sync version.

### Loading the Worker script (for async versions)
In each variant, the main module is bundled as one script file. However, the worker script is located in a separate file. The worker script is required to run the WebAssembly module in a Web Worker or Node.js worker thread. If you are using a build tool like Webpack, the worker script needs to be copied to the output directory. For example, in Webpack, you can use the `copy-webpack-plugin` to copy the worker script to the output directory. 

By default, the worker script is resolved under the same directory / URL prefix as the main module. If you want to change the location of the worker script, you can use pass the optional worker path parameter to the `setWorkerPath` function. For example:
```javascript
import kuzu from "kuzu-wasm";
kuzu.setWorkerPath('path/to/worker.js');
```

Note that this function must be called before any other function calls to the WebAssembly module. After the initialization is started, the worker script path cannot be changed and not finding the worker script will cause an error.

For the Node.js variant, the worker script can be resolved automatically and you do not need to set the worker path.

## API documentation
The API documentation can be found here:

**Synchronous** version: [API documentation](https://kuzudb.com/api-docs/wasm/sync/)

**Asynchronous** version: [API documentation](https://kuzudb.com/api-docs/wasm/async/)

## Local development

This section is relevant if you are interested in contributing to Kuzu's Wasm API. Note that you need to have Emscripten installed on your machine to build the WebAssembly module. You can follow the instructions [here](https://emscripten.org/docs/getting_started/downloads.html) to install Emscripten. You also need to have a C++ 20 compatible compiler installed on your machine to build the C++ code.

All the instructions below assume that you are in the `tools/wasm` directory.

### Install dependencies

```bash
npm i
```

### Build the WebAssembly module

```bash
npm run build
```

This will build the WebAssembly module in the `release` directory and create a tarball ready for publishing under the current directory. The tarball can be published to npm using the following command:

```bash
npm publish --access public
```

### Run API tests

```bash
npm test
```
