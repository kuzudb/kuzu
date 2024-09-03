const path = require("path");

describe("Extension loading", () => {
  const extensionPath = path.resolve(
    path.join(__dirname, "..", "..", "..", "extension", "httpfs", "build", "libhttpfs.kuzu_extension")
  );

  it("should install and load httpfs extension", async function () {
    this.timeout(10000);
    await conn.query("INSTALL httpfs");
    await conn.query(`LOAD EXTENSION "${extensionPath}"`);
  });
});
