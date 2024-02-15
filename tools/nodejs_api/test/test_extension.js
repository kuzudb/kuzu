const { assert } = require("chai");

describe("Extension loading", () => {
  it("should install and load httpfs extension", async function () {
    await conn.query("INSTALL httpfs");
    await conn.query("LOAD EXTENSION httpfs");
  });
});
