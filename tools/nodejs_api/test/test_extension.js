const { assert } = require("chai");

describe("Extension loading", () => {
  it("should install and load httpfs extension", async function () {
    this.timeout(10000);
    await conn.query("INSTALL httpfs");
    await conn.query("LOAD EXTENSION httpfs");
  });
});
