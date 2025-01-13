# kuzu-wasm
WebAssembly build of Kùzu in-process property graph database management system.

## Installation
```bash
npm i kuzu-wasm
```

## Package structure
In this package, three different variants of WebAssembly modules are provided:
- Default: This is the default build of the WebAssembly module. It does not support multithreading and uses Emscripten's default filesystem. This build has the smallest size and works in both Node.js and browser environments. It has the best compatibility and does not require cross-origin isolation. However, the performance maybe limited due to the lack of multithreading support. This build is located at the root level of the package.
- Multithreaded: This build supports multithreading and uses Emscripten's default filesystem. This build has a larger size compared to the default build and only requires [cross-origin isolation](https://web.dev/articles/cross-origin-isolation-guide) in the browser environment. This build is located in the `multithreaded` directory.
- Node.js: This build is optimized for Node.js and uses Node.js's filesystem instead of Emscripten's default filesystem (`NODEFS` flag is enabled). This build also supports multithreading. It is distributed as a CommonJS module rather than an ES module to maximize compatibility. This build is located in the `nodejs` directory. Note that this build only works in Node.js and does not work in the browser environment.

In each variant, there are two different versions of the WebAssembly module:
- Async: This version of the module is the default version and each function call returns a Promise. This version dispatches all the function calls to the WebAssembly module to a Web Worker or Node.js worker thread to prevent blocking the main thread. However, this version may have a slight overhead due to the serialization and deserialization of the data required by the worker threads. This version is located at the root level of each variant (e.g., `kuzu-wasm`, `kuzu-wasm/multithreaded`, `kuzu-wasm/nodejs`).
- Sync: This version of the module is synchronous and does not require any callbacks (other than the module initialization). This version is good for scripting / CLI / prototyping purposes but is not recommended to be used in GUI applications or web servers because it may block the main thread and cause unexpected freezes. This alternative version is located in the `sync` directory of each variant (e.g., `kuzu-wasm/sync`, `kuzu-wasm/multithreaded/sync`, `kuzu-wasm/nodejs/sync`).

Note that you cannot mix and match the variants and versions. For example, a `Database` object created with the default variant cannot be passed to a function in the multithreaded variant. Similarly, a `Database` object created with the async version cannot be passed to a function in the sync version.

## Loading the Worker script (for async versions)
In each variant, the main module is bundled as one script file. However, the worker script is located in a separate file. The worker script is required to run the WebAssembly module in a Web Worker or Node.js worker thread. If you are using a build tool like Webpack, the worker script needs to be copied to the output directory. For example, in Webpack, you can use the `copy-webpack-plugin` to copy the worker script to the output directory. 

By default, the worker script is resolved under the same directory / URL prefix as the main module. If you want to change the location of the worker script, you can use pass the optional worker path parameter to the `setWorkerPath` function. For example:
```javascript
import { setWorkerPath } from 'kuzu-wasm';
setWorkerPath('path/to/worker.js');
```

Note that this function must be called before any other function calls to the WebAssembly module. After the initialization is started, the worker script path cannot be changed and not finding the worker script will cause an error.

For the Node.js variant, the worker script can be resolved automatically and you do not need to set the worker path.

## Example usage

```js
// Import library
import * as kuzu from "kuzu-wasm";

(async () => {
  // Initialize the WebAssembly module
  await kuzu.init();

  // Write the data into WASM filesystem
  const userCSV = `Adam,30
Karissa,40
Zhang,50
Noura,25`;

  const cityCSV = `Waterloo,150000
Kitchener,200000
Guelph,75000`;

  const followsCSV = `Adam,Karissa,2020
Adam,Zhang,2020
Karissa,Zhang,2021
Zhang,Noura,2022`;

  const livesInCSV = `Adam,Waterloo
Karissa,Waterloo
Zhang,Kitchener
Noura,Guelph`;

  kuzu.FS.writeFile("user.csv", userCSV);
  kuzu.FS.writeFile("city.csv", cityCSV);
  kuzu.FS.writeFile("follows.csv", followsCSV);
  kuzu.FS.writeFile("lives-in.csv", livesInCSV);

  // Create an empty database and connect to it
  const db = new kuzu.Database("./test");
  const conn = new kuzu.Connection(db);

  // Create the tables
  await conn.query(
    "CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))"
  );
  await conn.query(
    "CREATE NODE TABLE City(name STRING, population INT64, PRIMARY KEY (name))"
  );
  await conn.query("CREATE REL TABLE Follows(FROM User TO User, since INT64)");
  await conn.query("CREATE REL TABLE LivesIn(FROM User TO City)");

  // Load the data
  await conn.query('COPY User FROM "user.csv"');
  await conn.query('COPY City FROM "city.csv"');
  await conn.query('COPY Follows FROM "follows.csv"');
  await conn.query('COPY LivesIn FROM "lives-in.csv"');

  const queryResult = await conn.query("MATCH (u:User) RETURN u.name, u.age;");

  // Get all rows from the query result
  const rows = await queryResult.getAllObjects();

  // Print the rows
  for (const row of rows) {
    console.log(row);
  }
})();
```

This script can be directly embedded in an HTML file, for example:
```html
<!DOCTYPE html>
<html>
<body>
    <h1>Welcome to WASM Test Server</h1>
    <script type="module">
    // Paste the script here
    </script>
</body>
</html>
```

## API documentation
The API documentation can be found [here](https://kuzudb.com/api-docs/wasm/).

## Local development (for contributors)
Build the WebAssembly module:
```bash
npm run build
```

This will build the WebAssembly module in the `release` directory and create a tarball ready for publishing under the current directory.

Run the tests:
```bash
npm test
```
