const { assert } = require("chai");

describe("Concurrent query execution within a single connection", function () {
  it("should dispatch multiple queries concurrently with query strings", async function () {
    const queryResults = await Promise.all([
      conn.query("MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;"),
      conn.query("MATCH (a:person) WHERE a.ID = 2 RETURN a.isStudent;"),
      conn.query("MATCH (a:person) WHERE a.ID = 3 RETURN a.isStudent;"),
      conn.query("MATCH (a:person) WHERE a.ID = 5 RETURN a.isStudent;"),
      conn.query("MATCH (a:person) WHERE a.ID = 7 RETURN a.isStudent;"),
    ]);
    const results = await Promise.all(
      queryResults.map((queryResult) => queryResult.getAllObjects())
    );
    assert.isTrue(results[0][0]["a.isStudent"]);
    assert.isTrue(results[1][0]["a.isStudent"]);
    assert.isFalse(results[2][0]["a.isStudent"]);
    assert.isFalse(results[3][0]["a.isStudent"]);
    assert.isFalse(results[4][0]["a.isStudent"]);
    for(const queryResult of queryResults) {
      await queryResult.close();
    }
  });

  it("should dispatch multiple queries concurrently with prepared statement", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.ID = $id RETURN a.isStudent;"
    );
    const queryResults = await Promise.all([
      conn.execute(preparedStatement, { id: 0 }),
      conn.execute(preparedStatement, { id: 2 }),
      conn.execute(preparedStatement, { id: 3 }),
      conn.execute(preparedStatement, { id: 5 }),
      conn.execute(preparedStatement, { id: 7 }),
    ]);
    const results = await Promise.all(
      queryResults.map((queryResult) => queryResult.getAllObjects())
    );
    assert.isTrue(results[0][0]["a.isStudent"]);
    assert.isTrue(results[1][0]["a.isStudent"]);
    assert.isFalse(results[2][0]["a.isStudent"]);
    assert.isFalse(results[3][0]["a.isStudent"]);
    assert.isFalse(results[4][0]["a.isStudent"]);
    await preparedStatement.close();
    for(const queryResult of queryResults) {
      await queryResult.close();
    }
  });
});

describe("Concurrent query execution across multiple connections", function () {
  it("should dispatch multiple queries concurrently on different connections", async function () {
    const connections = [];
    for (let i = 0; i < 5; i++) {
      connections.push(new kuzu.Connection(db));
    }
    const queryResults = await Promise.all([
      connections[0].query(
        "MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;"
      ),
      connections[1].query(
        "MATCH (a:person) WHERE a.ID = 2 RETURN a.isStudent;"
      ),
      connections[2].query(
        "MATCH (a:person) WHERE a.ID = 3 RETURN a.isStudent;"
      ),
      connections[3].query(
        "MATCH (a:person) WHERE a.ID = 5 RETURN a.isStudent;"
      ),
      connections[4].query(
        "MATCH (a:person) WHERE a.ID = 7 RETURN a.isStudent;"
      ),
    ]);
    const results = await Promise.all(
      queryResults.map((queryResult) => queryResult.getAllObjects())
    );
    assert.isTrue(results[0][0]["a.isStudent"]);
    assert.isTrue(results[1][0]["a.isStudent"]);
    assert.isFalse(results[2][0]["a.isStudent"]);
    assert.isFalse(results[3][0]["a.isStudent"]);
    assert.isFalse(results[4][0]["a.isStudent"]);
    for(const queryResult of queryResults) {
      await queryResult.close();
    }
    for(const connection of connections) {
      await connection.close();
    }
  });
});
