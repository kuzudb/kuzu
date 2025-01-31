# kuzu-wasm
Welcome to the documentation of `kuzu-wasm`, the WebAssembly build of Kùzu in-process property graph database management system. The documentation of each class can be found by clicking on the links in the sidebar.

## Additionally package-level functions:
- init()

Initialize the WebAssembly module. This function must be called before any other function in the module.

- getVersion()

Get the version of the WebAssembly module as a string.

- getStorageVersion()

Get the version of the storage format as a BigInt.

## File System API
The file system API is a direct binding to the `FS` module provided by Emscripten and available at the package level (`kuzu.FS`). By default, the file system is in-memory and is not persistent, but IDBFS can be mounted optionally to provide persistent storage. The API is used to interact with the file system of the WebAssembly module. Please refer to the overview of the [WebAssembly file system](https://emscripten.org/docs/porting/files/file_systems_overview.html) and the API documentation of the [FS module](https://emscripten.org/docs/api_reference/Filesystem-API.html) for more information.
