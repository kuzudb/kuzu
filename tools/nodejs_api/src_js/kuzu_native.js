/**
 * This file is a customized loader for the kuzujs.node native module.
 * It is used to load the native module with the correct flags on Linux so that
 * extension loading works correctly.
 * @module kuzu_native
 * @private
 */

const process = require("process");
const constants = require("constants");
const join = require("path").join;

const kuzuNativeModule = { exports: {} };
const modulePath = join(__dirname, "kuzujs.node");
if (process.platform === "linux") {
  process.dlopen(
    kuzuNativeModule,
    modulePath,
    constants.RTLD_LAZY | constants.RTLD_GLOBAL
  );
} else {
  process.dlopen(kuzuNativeModule, modulePath);
}

module.exports = kuzuNativeModule.exports;
