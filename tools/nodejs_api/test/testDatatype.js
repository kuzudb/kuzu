const { assert } = require("chai");

describe("BOOL", function () {
  it("should return Boolean for BOOL tpyes", function () {
    conn
      .execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;")
      .then((queryResult) => {
        return queryResult.all();
      })
      .then((result) => {
        assert.equal(result.length, 2);
        assert.equal(result[0].length, 1);
        assert.equal(result[1].length, 1);
        assert.equal(result[0][0], "isStudent");
        assert.equal(typeof result[1][0], "boolean");
        assert.equal(result[1][0], true);
      });
  });
});
