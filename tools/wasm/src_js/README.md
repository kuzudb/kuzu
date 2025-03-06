# kuzu-wasm
Welcome to the documentation of `kuzu-wasm`, the WebAssembly build of Kuzu in-process property graph database management system. 
You are currently viewing the *asynchronous* JavaScript API documentation. 
This version dispatches all the function calls to the WebAssembly module to a Web Worker or Node.js worker thread to prevent blocking the main thread. However, this version may have a slight overhead due to the serialization and deserialization of the data required by the worker threads. 

The documentation of each class can be found by clicking on the links in the sidebar.

For package-level functions, please refer to the documentation of the module `kuzu-wasm` from the sidebar.

A summary of the files in this module is listed below: