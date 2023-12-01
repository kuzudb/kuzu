const { assert } = require("chai");

describe("BOOL", function () {
  it("should transform booleans as BOOL parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.isStudent = $1 AND a.isWorker = $k RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: false,
      k: false,
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 1);
  });

  it("should transform number and strings as BOOL parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.isStudent = $1 AND a.isWorker = $k RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: 0,
      k: "f", // interpreted as true
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 4);
  });
});

describe("INT64", function () {
  it("should transform number as INT64 parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: 0,
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 1);
  });

  it("should reject other type as INT64 parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)"
    );
    try {
      await conn.execute(preparedStatement, {
        1: "0",
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: true,
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: new Date(),
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }
  });
});

describe("INT32", function () {
  it("should transform number as INT32 parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:movies) WHERE a.length > $1 RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: 200.0,
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 2);
  });

  it("should reject other type as INT32 parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:movies) WHERE a.length > $1 RETURN COUNT(*)"
    );
    try {
      await conn.execute(preparedStatement, {
        1: "200",
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: true,
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: new Date(),
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }
  });
});

describe("INT16", function () {
  it("should transform number as INT16 parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.length > $1 RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: 10,
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 2);
  });

  it("should reject other type as INT16 parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.length > $1 RETURN COUNT(*)"
    );
    try {
      await conn.execute(preparedStatement, {
        1: "10",
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: true,
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: new Date(),
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }
  });
});

describe("DOUBLE", function () {
  it("should transform number as DOUBLE parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.eyeSight > $1 RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: 4.5,
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 7);
  });

  it("should reject other type as DOUBLE parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.eyeSight > $1 RETURN COUNT(*)"
    );
    try {
      await conn.execute(preparedStatement, {
        1: "4.5",
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: true,
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: new Date(),
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }
  });
});

describe("FLOAT", function () {
  it("should transform number as FLOAT parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.height < $1 RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: 0.999,
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 1);
  });

  it("should reject other type as FLOAT parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.height < $1 RETURN COUNT(*)"
    );
    try {
      await conn.execute(preparedStatement, {
        1: "0.999",
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: true,
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: new Date(),
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }
  });
});

describe("STRING", function () {
  it("should transform string as STRING parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.fName = $1 RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: "Alice",
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 1);
  });

  it("should transform other type as STRING parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.fName = $1 RETURN COUNT(*)"
    );
    let queryResult = await conn.execute(preparedStatement, {
      1: 1,
    });
    let result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 0);

    queryResult = await conn.execute(preparedStatement, {
      1: true,
    });
    result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 0);

    queryResult = await conn.execute(preparedStatement, {
      1: new Date(),
    });
    result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 0);
  });
});

describe("DATE", function () {
  it("should transform date as DATE parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.birthdate > $1 RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: new Date(0),
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 4);
  });

  it("should reject other type as DATE parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.birthdate > $1 RETURN COUNT(*)"
    );
    try {
      await conn.execute(preparedStatement, {
        1: "1970-01-01",
      });
    } catch (e) {
      assert.equal(e.message, "Expected a date for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: 0,
      });
    } catch (e) {
      assert.equal(e.message, "Expected a date for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: true,
      });
    } catch (e) {
      assert.equal(e.message, "Expected a date for parameter 1.");
    }
  });
});

describe("TIMESTAMP", function () {
  it("should transform date as TIMESTAMP parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.registerTime > $1 RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: new Date(0),
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 7);
  });

  it("should reject other type as TIMESTAMP parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.registerTime > $1 RETURN COUNT(*)"
    );
    try {
      await conn.execute(preparedStatement, {
        1: "1970-01-01",
      });
    } catch (e) {
      assert.equal(e.message, "Expected a date for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: 0,
      });
    } catch (e) {
      assert.equal(e.message, "Expected a date for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: true,
      });
    } catch (e) {
      assert.equal(e.message, "Expected a date for parameter 1.");
    }
  });
});

describe("INTERVAL", function () {
  it("should transform number as INTERVAL parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.lastJobDuration > $1 RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: 0,
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 8);
  });

  it("should reject other type as INTERVAL parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.lastJobDuration > $1 RETURN COUNT(*)"
    );
    try {
      await conn.execute(preparedStatement, {
        1: "1970-01-01",
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: new Date(),
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }

    try {
      await conn.execute(preparedStatement, {
        1: true,
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }
  });
});
