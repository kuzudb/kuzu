const { assert } = require("chai");

describe("Connection constructor", function () {
  it("should create a connection with a valid database object", async function () {
    const connection = new kuzu.Connection(db);
    assert.exists(connection);
    assert.equal(connection.constructor.name, "Connection");
    await connection.init();
    assert.exists(connection._connection);
    assert.isTrue(connection._isInitialized);
    assert.notExists(connection._initPromise);
  });

  it("should throw error if the database object is invalid", async function () {
    try {
      const _ = new kuzu.Connection({});
      assert.fail("No error thrown when the database object is invalid.");
    } catch (e) {
      assert.equal(e.message, "database must be a valid Database object.");
    }
  });
});

describe("Prepare", function () {
  it("should prepare a valid statement", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)"
    );
    assert.exists(preparedStatement);
    assert.isTrue(preparedStatement.isSuccess());
    assert.equal(preparedStatement.getErrorMessage(), "");
  });

  it("should return error message if the statement is invalid", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:dog) WHERE a.ID = $1 RETURN COUNT(*)"
    );
    assert.exists(preparedStatement);
    assert.isFalse(preparedStatement.isSuccess());
    assert.equal(
      preparedStatement.getErrorMessage(),
      "Binder exception: Node table dog does not exist."
    );
  });

  it("should return error message if the statement is not a string", async function () {
    try {
      const _ = await conn.prepare({});
      assert.fail("No error thrown when the query is not a string.");
    } catch (e) {
      assert.equal(e.message, "statement must be a string.");
    }
  });

  it("should throw error if the statement is not a string", async function () {
    try {
      const _ = await conn.prepare({});
      assert.fail("No error thrown when the query is not a string.");
    } catch (e) {
      assert.equal(e.message, "statement must be a string.");
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
    const tuple = await queryResult.getNext();
    assert.exists(tuple);
    assert.exists(tuple["COUNT_STAR()"]);
    assert.equal(tuple["COUNT_STAR()"], 1);
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
      assert.equal(
        e.message,
        "Binder exception: Node table dog does not exist."
      );
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
        "preparedStatement must be a valid PreparedStatement object."
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
    try {
      const preparedStatement = await conn.prepare(
        "MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)"
      );
      assert.exists(preparedStatement);
      assert.isTrue(preparedStatement.isSuccess());
      await conn.execute(preparedStatement, { 1: [] });
      assert.fail("No error thrown when a parameter is not valid.");
    } catch (e) {
      assert.equal(
        e.message,
        "The value of each parameter must be a boolean, number, string, or Date object."
      );
    }
  });

  it("should throw error if a parameter is undefined or null", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)"
    );
    assert.exists(preparedStatement);
    assert.isTrue(preparedStatement.isSuccess());
    try {
      await conn.execute(preparedStatement, { 1: undefined });
      assert.fail("No error thrown when a parameter is undefined.");
    } catch (e) {
      assert.equal(
        e.message,
        "The value of each parameter must not be null or undefined."
      );
    }
    try {
      await conn.execute(preparedStatement, { 1: null });
      assert.fail("No error thrown when a parameter is null.");
    } catch (e) {
      assert.equal(
        e.message,
        "The value of each parameter must not be null or undefined."
      );
    }
  });
});

describe("Query", function () {
  it("should run a valid query", async function () {
    const queryResult = await conn.query("MATCH (a:person) RETURN COUNT(*)");
    assert.exists(queryResult);
    assert.equal(queryResult.constructor.name, "QueryResult");
    assert.isTrue(queryResult.hasNext());
    const tuple = await queryResult.getNext();
    assert.exists(tuple);
    assert.exists(tuple["COUNT_STAR()"]);
    assert.equal(tuple["COUNT_STAR()"], 8);
  });

  it("should throw error if the statement is invalid", async function () {
    try {
      await conn.query("MATCH (a:dog) RETURN COUNT(*)");
      assert.fail("No error thrown when the query is invalid.");
    } catch (e) {
      assert.equal(
        e.message,
        "Binder exception: Node table dog does not exist."
      );
    }
  });

  it("should throw error if the statement is not a string", async function () {
    try {
      const _ = await conn.query(42);
      assert.fail("No error thrown when the query is not a string.");
    } catch (e) {
      assert.equal(e.message, "statement must be a string.");
    }
  });
});

describe("Get node table names", function () {
  it("should return node table names", async function () {
    const nodeTableNames = await conn.getNodeTableNames();
    assert.exists(nodeTableNames);
    assert.isArray(nodeTableNames);
    nodeTableNames.sort();
    assert.equal(nodeTableNames.length, 3);
    assert.deepEqual(nodeTableNames, ["movies", "organisation", "person"]);
  });
});

describe("Get rel table names", function () {
  it("should return rel table names", async function () {
    const relTableNames = await conn.getRelTableNames();
    relTableNames.sort();
    assert.exists(relTableNames);
    assert.isArray(relTableNames);
    assert.equal(relTableNames.length, 5);
    assert.deepEqual(relTableNames, [
      "knows",
      "marries",
      "meets",
      "studyAt",
      "workAt",
    ]);
  });
});

describe("Get node property names", function () {
  it("should return node property names for a valid node table", async function () {
    const EXPECTED_NODE_PROPERTY_NAMES = [
      { name: "ID", type: "INT64", isPrimaryKey: true },
      { name: "age", type: "INT64", isPrimaryKey: false },
      { name: "birthdate", type: "DATE", isPrimaryKey: false },
      {
        name: "courseScoresPerTerm",
        type: "INT64[][]",
        isPrimaryKey: false,
      },
      { name: "eyeSight", type: "DOUBLE", isPrimaryKey: false },
      { name: "fName", type: "STRING", isPrimaryKey: false },
      { name: "gender", type: "INT64", isPrimaryKey: false },
      { name: "grades", type: "INT64[4]", isPrimaryKey: false },
      { name: "height", type: "FLOAT", isPrimaryKey: false },
      { name: "isStudent", type: "BOOL", isPrimaryKey: false },
      { name: "isWorker", type: "BOOL", isPrimaryKey: false },
      { name: "lastJobDuration", type: "INTERVAL", isPrimaryKey: false },
      { name: "registerTime", type: "TIMESTAMP", isPrimaryKey: false },
      { name: "usedNames", type: "STRING[]", isPrimaryKey: false },
      { name: "workedHours", type: "INT64[]", isPrimaryKey: false },
    ];
    const nodePropertyNames = await conn.getNodePropertyNames("person");
    assert.exists(nodePropertyNames);
    assert.isArray(nodePropertyNames);
    nodePropertyNames.sort((a, b) => (a.name > b.name ? 1 : -1));
    assert.equal(nodePropertyNames.length, 15);

    for (let i = 0; i < nodePropertyNames.length; i++) {
      assert.equal(
        nodePropertyNames[i].name,
        EXPECTED_NODE_PROPERTY_NAMES[i].name
      );
      assert.equal(
        nodePropertyNames[i].type,
        EXPECTED_NODE_PROPERTY_NAMES[i].type
      );
      assert.equal(
        nodePropertyNames[i].isPrimaryKey,
        EXPECTED_NODE_PROPERTY_NAMES[i].isPrimaryKey
      );
    }
  });

  it("should throw error if the node table does not exist", async function () {
    try {
      await conn.getNodePropertyNames("dog");
      assert.fail("No error thrown when the node table does not exist.");
    } catch (e) {
      assert.equal(e.message, "Runtime exception: Cannot find node table dog");
    }
  });
});

describe("Get rel property names", function () {
  it("should return rel property names for a valid rel table", async function () {
    const EXPECTED_REL_PROPERTY_NAMES = {
      props: [
        { name: "comments", type: "STRING[]" },
        { name: "date", type: "DATE" },
        { name: "meetTime", type: "TIMESTAMP" },
        { name: "validInterval", type: "INTERVAL" },
      ],
      src: "person",
      name: "knows",
      dst: "person",
    };

    const relPropertyNames = await conn.getRelPropertyNames("knows");
    assert.exists(relPropertyNames);
    assert.exists(relPropertyNames.props);
    relPropertyNames.props.sort((a, b) => (a.name > b.name ? 1 : -1));
    assert.equal(relPropertyNames.props.length, 4);
    relPropertyNames.props.forEach((prop, i) => {
      assert.equal(prop.name, EXPECTED_REL_PROPERTY_NAMES.props[i].name);
      assert.equal(prop.type, EXPECTED_REL_PROPERTY_NAMES.props[i].type);
    });
    assert.equal(relPropertyNames.src, EXPECTED_REL_PROPERTY_NAMES.src);
    assert.equal(relPropertyNames.name, EXPECTED_REL_PROPERTY_NAMES.name);
    assert.equal(relPropertyNames.dst, EXPECTED_REL_PROPERTY_NAMES.dst);
  });

  it("should throw error if the rel table does not exist", async function () {
    try {
      await conn.getRelPropertyNames("friend");
      assert.fail("No error thrown when the rel table does not exist.");
    } catch (e) {
      assert.equal(
        e.message,
        "Runtime exception: Cannot find rel table friend"
      );
    }
  });
});
