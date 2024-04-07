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

describe("INT8", function () {
  it("should transform number as INT8 parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (m:movies) WHERE m.description.stars > $1 RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: 10,
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 1);
  });

  it("should reject other type as INT8 parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (m:movies) WHERE m.description.stars > $1 RETURN COUNT(*)"
    );
    try {
      await conn.execute(preparedStatement, {
        1: "4",
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }
  });
});

describe("UINT64", function () {
  it("should transform number as UINT64 parameter", async function () {
    const preparedStatement = await conn.prepare("RETURN CAST($1, 'UINT64')");
    const queryResult = await conn.execute(preparedStatement, {
      1: 10000000000000000000,
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["CAST($1, UINT64)"], "10000000000000000000");
  });

  it("should reject other type as UINT64 parameter", async function () {
    const preparedStatement = await conn.prepare("RETURN CAST($1, 'UINT64')");
    try {
      await conn.execute(preparedStatement, {
        1: "10000000000000000000",
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }
  });
});

describe("UINT32", function () {
  it("should transform number as UINT32 parameter", async function () {
    const preparedStatement = await conn.prepare("RETURN CAST($1, 'UINT32')");
    const queryResult = await conn.execute(preparedStatement, {
      1: 4294967295,
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["CAST($1, UINT32)"], "4294967295");
  });

  it("should reject other type as UINT32 parameter", async function () {
    const preparedStatement = await conn.prepare("RETURN CAST($1, 'UINT32')");
    try {
      await conn.execute(preparedStatement, {
        1: "4294967295",
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }
  });
});

describe("UINT16", function () {
  it("should transform number as UINT16 parameter", async function () {
    const preparedStatement = await conn.prepare("RETURN CAST($1, 'UINT16')");
    const queryResult = await conn.execute(preparedStatement, {
      1: 65535,
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["CAST($1, UINT16)"], "65535");
  });

  it("should reject other type as UINT16 parameter", async function () {
    const preparedStatement = await conn.prepare("RETURN CAST($1, 'UINT16')");
    try {
      await conn.execute(preparedStatement, {
        1: "65535",
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }
  });
});

describe("UINT8", function () {
  it("should transform number as UINT8 parameter", async function () {
    const preparedStatement = await conn.prepare("RETURN CAST($1, 'UINT8')");
    const queryResult = await conn.execute(preparedStatement, {
      1: 255,
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["CAST($1, UINT8)"], "255");
  });

  it("should reject other type as UINT8 parameter", async function () {
    const preparedStatement = await conn.prepare("RETURN CAST($1, 'UINT8')");
    try {
      await conn.execute(preparedStatement, {
        1: "255",
      });
    } catch (e) {
      assert.equal(e.message, "Expected a number for parameter 1.");
    }
  });
});

//TODO(Chang): fix me
// describe("INT128", function () {
//   it("should transform single-word positive BigInt as INT128 parameter", async function () {
//     const preparedStatement = await conn.prepare("RETURN CAST($1, 'STRING')");
//     const queryResult = await conn.execute(preparedStatement, {
//       1: BigInt("123456789"),
//     });
//     const result = await queryResult.getAll();
//     assert.equal(result[0]["CAST($1, STRING)"], "123456789");
//   });
//
//   it("should transform single-word negative BigInt as INT128 parameter", async function () {
//     const preparedStatement = await conn.prepare("RETURN CAST($1, 'STRING')");
//     const queryResult = await conn.execute(preparedStatement, {
//       1: BigInt("-123456789"),
//     });
//     const result = await queryResult.getAll();
//     assert.equal(result[0]["CAST($1, STRING)"], "-123456789");
//   });
//
//   it("should transform two-word positive BigInt as INT128 parameter", async function () {
//     const preparedStatement = await conn.prepare("RETURN CAST($1, 'STRING')");
//     const queryResult = await conn.execute(preparedStatement, {
//       1: BigInt("18446744073709551610"),
//     });
//     const result = await queryResult.getAll();
//     assert.equal(result[0]["CAST($1, STRING)"], "18446744073709551610");
//   });
//
//   it("should transform two-word negative BigInt as INT128 parameter", async function () {
//     const preparedStatement = await conn.prepare("RETURN CAST($1, 'STRING')");
//     const queryResult = await conn.execute(preparedStatement, {
//       1: BigInt("-18446744073709551610"),
//     });
//     const result = await queryResult.getAll();
//     assert.equal(result[0]["CAST($1, STRING)"], "-18446744073709551610");
//   });
//
//   it("should reject other type as INT128 parameter", async function () {
//     const preparedStatement = await conn.prepare("RETURN CAST($1, 'STRING')");
//     try {
//       await conn.execute(preparedStatement, {
//         1: "123456789",
//       });
//     } catch (e) {
//       assert.equal(e.message, "Expected a BigInt for parameter 1.");
//     }
//   });
// });

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

describe("TIMESTAMP_TZ", function () {
  it("should transform date as TIMESTAMP_TZ parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:movies) WHERE a.description.release_tz > $1 RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: new Date(0),
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 3);
  });

  it("should reject other type as TIMESTAMP_TZ parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:movies) WHERE a.description.release_tz > $1 RETURN COUNT(*)"
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

describe("TIMESTAMP_NS", function () {
  it("should transform date as TIMESTAMP_NS parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:movies) WHERE a.description.release_ns > $1 RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: new Date(0),
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 3);
  });

  it("should reject other type as TIMESTAMP_NS parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:movies) WHERE a.description.release_ns > $1 RETURN COUNT(*)"
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

describe("TIMESTAMP_MS", function () {
  it("should transform date as TIMESTAMP_MS parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:movies) WHERE a.description.release_ms > $1 RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: new Date(0),
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 3);
  });

  it("should reject other type as TIMESTAMP_MS parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:movies) WHERE a.description.release_ms > $1 RETURN COUNT(*)"
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

describe("TIMESTAMP_SEC", function () {
  it("should transform date as TIMESTAMP_SEC parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:movies) WHERE a.description.release_sec > $1 RETURN COUNT(*)"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: new Date(0),
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["COUNT_STAR()"], 3);
  });

  it("should reject other type as TIMESTAMP_SEC parameter", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:movies) WHERE a.description.release_sec > $1 RETURN COUNT(*)"
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

describe("UUID", function () {
  it("should transform string as UUID parameter", async function () {
    const preparedStatement = await conn.prepare(
      "RETURN CAST($1, 'STRING') AS UUID"
    );
    const queryResult = await conn.execute(preparedStatement, {
      1: "123e4567-e89b-12d3-a456-426614174000",
    });
    const result = await queryResult.getAll();
    assert.equal(result[0]["UUID"], "123e4567-e89b-12d3-a456-426614174000");
  });
});
