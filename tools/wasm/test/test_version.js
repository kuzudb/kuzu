const { assert } = require("chai");

describe("Get version", function () {
  it("should get the version of the library", async function () {
    const version = await kuzu.getVersion();
    assert.isString(version);
    assert.notEqual(version, "");
  });

  it("should get the storage version of the library", async function () {
    const storageVersion = await kuzu.getStorageVersion();
    assert.isTrue(storageVersion > 0);
  });
});
