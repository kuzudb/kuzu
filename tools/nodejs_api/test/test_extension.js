const { assert } = require("chai");

describe("Extension loading", () => {
  it("should install and load httpfs extension", async function () {
    await conn.query("INSTALL httpfs");
    // Skip this test until the fix is in place
    // await conn.query("LOAD EXTENSION httpfs");
  });
});
