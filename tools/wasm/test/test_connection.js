const { assert } = require("chai");

describe("Connection constructor", function () {
  it("should create a connection with a valid database object", async function () {
    const connection = new kuzu.Connection(db);
    assert.exists(connection);
    assert.equal(connection.constructor.name, "Connection");
    await connection.init();
    assert.isTrue(connection._isInitialized);
    assert.notExists(connection._initPromise);
    await connection.close();
  });
});

describe("Prepare", function () {
  it("should prepare a valid statement", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)"
    );
    assert.exists(preparedStatement);
    assert.isTrue(preparedStatement.isSuccess());
    assert.equal((await preparedStatement.getErrorMessage()), "");
    await preparedStatement.close();
  });

  it("should return error message if the statement is invalid", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:dog) WHERE a.ID = $1 RETURN COUNT(*)"
    );
    assert.exists(preparedStatement);
    assert.isFalse(preparedStatement.isSuccess());
    assert.equal(
      (await preparedStatement.getErrorMessage()),
      "Binder exception: Table dog does not exist."
    );
  });

  it("should return error message if the statement is not a string", async function () {
    try {
      const _ = await conn.prepare({});
      assert.fail("No error thrown when the query is not a string.");
    } catch (e) {
      assert.equal(e.message, "Statement must be a string.");
    }
  });
});

describe("Execute", function () {
  it("should execute a valid prepared statement", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)"
    );
    assert.exists(preparedStatement);
    assert.isTrue(preparedStatement.isSuccess());
    const queryResult = await conn.execute(preparedStatement, { 1: 0 });
    assert.exists(queryResult);
    assert.equal(queryResult.constructor.name, "QueryResult");
    assert.isTrue(queryResult.hasNext());
    const tuples = await queryResult.getAllObjects();
    assert.equal(tuples.length, 1);
    const tuple = tuples[0];
    assert.exists(tuple);
    assert.exists(tuple["COUNT_STAR()"]);
    assert.equal(tuple["COUNT_STAR()"], 1);
    await preparedStatement.close();
    await queryResult.close();
  });

  it("should throw error if the prepared statement is invalid", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:dog) WHERE a.ID = $1 RETURN COUNT(*)"
    );
    assert.exists(preparedStatement);
    assert.isFalse(preparedStatement.isSuccess());
    try {
      await conn.execute(preparedStatement, { 1: 0 });
      assert.fail("No error thrown when the prepared statement is invalid.");
    } catch (e) {
      assert.equal(e.message, "Binder exception: Table dog does not exist.");
    }
  });

  it("should throw error if the prepared statement is not a PreparedStatement object", async function () {
    try {
      const _ = await conn.execute({}, { 1: 0 });
      assert.fail(
        "No error thrown when the prepared statement is not a PreparedStatement object."
      );
    } catch (e) {
      assert.equal(
        e.message,
        "Prepared statement not found"
      );
    }
  });

  it("should throw error if the parameters is not a plain object", async function () {
    try {
      const preparedStatement = await conn.prepare(
        "MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)"
      );
      assert.exists(preparedStatement);
      assert.isTrue(preparedStatement.isSuccess());
      await conn.execute(preparedStatement, []);
      assert.fail("No error thrown when params is not a plain object.");
    } catch (e) {
      assert.equal(e.message, "params must be a plain object.");
    }
  });

  it("should throw error if a parameter is not valid", async function () {
    let preparedStatement;
    try {
      preparedStatement = await conn.prepare(
        "MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)"
      );
      assert.exists(preparedStatement);
      assert.isTrue(preparedStatement.isSuccess());
      await conn.execute(preparedStatement, { 1: [] });
      assert.fail("No error thrown when a parameter is not valid.");
    } catch (e) {
      assert.equal(
        e.message,
        "Unsupported type"
      );
    } finally {
      await preparedStatement.close();
    }
  });
});

describe("Query", function () {
  it("should run a valid query", async function () {
    const queryResult = await conn.query("MATCH (a:person) RETURN COUNT(*)");
    assert.exists(queryResult);
    assert.equal(queryResult.constructor.name, "QueryResult");
    assert.isTrue(queryResult.hasNext());
    const tuples = await queryResult.getAllObjects();
    assert.equal(tuples.length, 1);
    const tuple = tuples[0];
    assert.exists(tuple);
    assert.exists(tuple["COUNT_STAR()"]);
    assert.equal(tuple["COUNT_STAR()"], 8);
  });

  it("should throw error if the statement is invalid", async function () {
    try {
      await conn.query("MATCH (a:dog) RETURN COUNT(*)");
      assert.fail("No error thrown when the query is invalid.");
    } catch (e) {
      assert.equal(e.message, "Binder exception: Table dog does not exist.");
    }
  });

  it("should throw error if the statement is not a string", async function () {
    try {
      const _ = await conn.query(42);
      assert.fail("No error thrown when the query is not a string.");
    } catch (e) {
      assert.equal(e.message, "Statement must be a string.");
    }
  });

  it("should be able to run multiple queries", async function () {
    const queryResult = await conn.query(`
      RETURN 1;
      RETURN 2;
      RETURN 3;
    `);
    assert.exists(queryResult);
    const tuples = await queryResult.getAllObjects();
    assert.equal(tuples.length, 1);
    assert.equal(tuples[0]['1'], 1);

    assert.isTrue(queryResult.hasNextQueryResult());
    const queryResult2 = await queryResult.getNextQueryResult();
    assert.exists(queryResult2);
    const tuples2 = await queryResult2.getAllObjects();
    assert.equal(tuples2.length, 1);
    assert.equal(tuples2[0]['2'], 2);

    assert.isTrue(queryResult2.hasNextQueryResult());
    const queryResult3 = await queryResult2.getNextQueryResult();
    assert.exists(queryResult3);
    const tuples3 = await queryResult3.getAllObjects();
    assert.equal(tuples3.length, 1);
    assert.equal(tuples3[0]['3'], 3);
    assert.isFalse(queryResult3.hasNextQueryResult());
    await queryResult.close();
  });

  it("should throw error if one of the multiple queries is invalid", async function () {
    try {
      const _ = await conn.query(`
        RETURN 1;
        RETURN 2;
        MATCH (a:dog) RETURN COUNT(*);
      `);

    } catch (e) {
      assert.equal(e.message, "Binder exception: Table dog does not exist.");
    }
  });
});

describe("Close", function () {
  it("should close the connection", async function () {
    const newConn = new kuzu.Connection(db);
    await newConn.init();
    await newConn.close();
    assert.isTrue(newConn._isClosed);
    assert.notExists(newConn._connection);
    try {
      await newConn.query("MATCH (a:person) RETURN COUNT(*)");
      assert.fail("No error thrown when the connection is closed.");
    } catch (e) {
      assert.equal(e.message, "Connection is closed.");
    }
  });
});
