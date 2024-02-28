const { assert } = require("chai");

describe("Get version", function () {
  it("should get the version of the library", function () {
    assert.isString(kuzu.VERSION);
    assert.notEqual(kuzu.VERSION, "");
  });

  it("should get the storage version of the library", function () {
    assert.isNumber(kuzu.STORAGE_VERSION);
    assert.isAtLeast(kuzu.STORAGE_VERSION, 1);
  });
});
