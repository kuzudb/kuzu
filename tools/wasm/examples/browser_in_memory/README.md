# Kuzu WebAssembly In-Browser Example
This example demonstrates how to use the Kuzu WebAssembly build in a web browser. The example uses the asynchronous JavaScript API to interact with the WebAssembly module.

## Usage
### Install Dependencies
```bash
npm i
```

### Bootstrap (copy the WebAssembly module)
```bash
npm run bootstrap
```

### Start the web server
```bash
npm run serve
```

### Open the browser
Navigate to `http://localhost:3000` in your web browser.

## Source Code
The source code for this example can be found in the `public/index.html` file. The file is a simple HTML file that includes the JavaScript code to interact with the WebAssembly module. Vanilla JavaScript is used and only kuzu-wasm is imported. No other dependencies are used.
