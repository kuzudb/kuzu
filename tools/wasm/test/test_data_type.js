const { assert } = require("chai");
const EPSILON = 1e-6;

describe("BOOL", function () {
  it("should convert BOOL type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.isStudent" in result[0]);
    assert.equal(typeof result[0]["a.isStudent"], "boolean");
    assert.isTrue(result[0]["a.isStudent"]);
    await queryResult.close();
  });
});

describe("INT8", function () {
  it("should convert INT8 type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE a.ID = 0 RETURN s.level"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("s.level" in result[0]);
    assert.equal(result[0]["s.level"], 5);
    await queryResult.close();
  });
});

describe("INT16", function () {
  it("should convert INT16 type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE a.ID = 0 RETURN s.length"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("s.length" in result[0]);
    assert.equal(result[0]["s.length"], 5);
    await queryResult.close();
  });
});

describe("INT32", function () {
  it("should convert INT32 type", async function () {
    const queryResult = await conn.query(
      'MATCH (a:movies) WHERE a.name = "Roma" RETURN a.length'
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.length" in result[0]);
    assert.equal(result[0]["a.length"], 298);
    await queryResult.close();
  });
});

describe("INT64", function () {
  it("should convert INT64 type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.ID;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.ID" in result[0]);
    assert.equal(result[0]["a.ID"], 0);
    await queryResult.close();
  });
});

describe("UINT8", function () {
  it("should convert UINT8 type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE a.ID = 0 RETURN s.ulevel"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("s.ulevel" in result[0]);
    assert.equal(result[0]["s.ulevel"], 250);
    await queryResult.close();
  });
});

describe("UINT16", function () {
  it("should convert UINT16 type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE a.ID = 0 RETURN s.ulength"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("s.ulength" in result[0]);
    assert.equal(result[0]["s.ulength"], 33768);
    await queryResult.close();
  });
});

describe("UINT32", function () {
  it("should convert UINT32 type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE a.ID = 0 RETURN s.temperature"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("s.temperature" in result[0]);
    assert.equal(result[0]["s.temperature"], 32800);
    await queryResult.close();
  });
});

describe("UINT64", function () {
  it("should convert UINT64 type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE a.ID = 0 RETURN s.code"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("s.code" in result[0]);
    assert.equal(result[0]["s.code"], 9223372036854775808);
    await queryResult.close();
  });
});

describe("SERIAL", function () {
  it("should convert SERIAL type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:moviesSerial) WHERE a.ID = 2 RETURN a.ID;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.ID" in result[0]);
    assert.equal(result[0]["a.ID"], 2);
    await queryResult.close();
  });
});

describe("FLOAT", function () {
  it("should convert FLOAT type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.height;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.height" in result[0]);
    assert.equal(typeof result[0]["a.height"], "number");
    assert.approximately(result[0]["a.height"], 1.731, EPSILON);
    await queryResult.close();
  });
});

describe("DOUBLE", function () {
  it("should convert DOUBLE type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.eyeSight;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.eyeSight" in result[0]);
    assert.equal(typeof result[0]["a.eyeSight"], "number");
    assert.approximately(result[0]["a.eyeSight"], 5.0, EPSILON);
    await queryResult.close();
  });
});

describe("STRING", function () {
  it("should convert STRING type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.fName;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.fName" in result[0]);
    assert.equal(typeof result[0]["a.fName"], "string");
    assert.equal(result[0]["a.fName"], "Alice");
    await queryResult.close();
  });
});

describe("BLOB", function () {
  it("should convert BLOB type", async function () {
    const queryResult = await conn.query(
      "RETURN BLOB('\\\\xAA\\\\xBB\\\\xCD\\\\x1A');"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    const value = Object.values(result[0])[0];
    assert.isTrue(value instanceof Uint8Array);
    assert.equal(value.length, 4);
    assert.equal(value[0], 0xaa);
    assert.equal(value[1], 0xbb);
    assert.equal(value[2], 0xcd);
    assert.equal(value[3], 0x1a);
    await queryResult.close();
  });
});

describe("UUID", function () {
  it("should convert UUID type", async function () {
    const queryResult = await conn.query(
      "RETURN UUID('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11');"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    const value = Object.values(result[0])[0];
    assert.equal(typeof value, "string");
    assert.equal(value, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    await queryResult.close();
  });
});

describe("DATE", function () {
  it("should convert DATE type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.birthdate;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.birthdate" in result[0]);
    assert.equal(typeof result[0]["a.birthdate"], "object");
    assert.equal(
      Number(result[0]["a.birthdate"]),
      Number(new Date("1900-01-01"))
    );
    await queryResult.close();
  });

  it("should convert DATE type for very old date", async function () {
    const queryResult = await conn.query(
      "RETURN MAKE_DATE(1000,11,22);"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    const value = Object.values(result[0])[0];
    assert.equal(typeof value, "object");
    const isoString = value.toISOString();
    assert.equal(isoString, "1000-11-22T00:00:00.000Z");
    await queryResult.close();
  });

  it("should convert DATE type for very future date", async function () {
    const queryResult = await conn.query(
      "RETURN MAKE_DATE(9999,11,22);"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    const value = Object.values(result[0])[0];
    assert.equal(typeof value, "object");
    const isoString = value.toISOString();
    assert.equal(isoString, "9999-11-22T00:00:00.000Z");
    await queryResult.close();
  });
});

describe("TIMESTAMP", function () {
  it("should convert TIMESTAMP type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.registerTime;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.registerTime" in result[0]);
    assert.equal(typeof result[0]["a.registerTime"], "object");
    assert.equal(
      Number(result[0]["a.registerTime"].getTime()),
      Number(new Date("2011-08-20 11:25:30Z"))
    );
    await queryResult.close();
  });
});

describe("TIMESTAMP_TZ", function () {
  it("should convert TIMESTAMP_TZ type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:movies) WHERE a.length = 126 RETURN a.description;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.description" in result[0]);
    assert.isTrue("release_tz" in result[0]["a.description"]);
    assert.equal(typeof result[0]["a.description"]["release_tz"], "object");
    assert.equal(
      Number(result[0]["a.description"]["release_tz"].getTime()),
      Number(new Date("2011-08-20 11:25:30.123Z"))
    );
    await queryResult.close();
  });
});

describe("TIMESTAMP_NS", function () {
  it("should convert TIMESTAMP_NS type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:movies) WHERE a.length = 126 RETURN a.description;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.description" in result[0]);
    assert.isTrue("release_ns" in result[0]["a.description"]);
    assert.equal(typeof result[0]["a.description"]["release_ns"], "object");
    assert.equal(
      Number(result[0]["a.description"]["release_ns"].getTime()),
      Number(new Date("2011-08-20 11:25:30.123Z"))
    );
    await queryResult.close();
  });
});

describe("TIMESTAMP_MS", function () {
  it("should convert TIMESTAMP_MS type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:movies) WHERE a.length = 126 RETURN a.description;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.description" in result[0]);
    assert.isTrue("release_ms" in result[0]["a.description"]);
    assert.equal(typeof result[0]["a.description"]["release_ms"], "object");
    assert.equal(
      Number(result[0]["a.description"]["release_ms"].getTime()),
      Number(new Date("2011-08-20 11:25:30.123Z"))
    );
    await queryResult.close();
  });
});

describe("TIMESTAMP_SEC", function () {
  it("should convert TIMESTAMP_SEC type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:movies) WHERE a.length = 126 RETURN a.description;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.description" in result[0]);
    assert.isTrue("release_sec" in result[0]["a.description"]);
    assert.equal(typeof result[0]["a.description"]["release_sec"], "object");
    assert.equal(
      Number(result[0]["a.description"]["release_sec"].getTime()),
      Number(new Date("2011-08-20 11:25:30Z"))
    );
    await queryResult.close();
  });
});

describe("INTERVAL", function () {
  it("should convert INTERVAL type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.lastJobDuration;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.lastJobDuration" in result[0]);
    assert.equal(
      result[0]["a.lastJobDuration"],
      /* 3 years 2 days 13 hours 2 minutes */
      (((3 * 12 * 30 + 2) * 24 + 13) * 60 + 2) * 60 * 1000
    );
    await queryResult.close();
  });
});

describe("LIST", function () {
  it("should convert LIST type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.courseScoresPerTerm;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.courseScoresPerTerm" in result[0]);
    assert.isTrue(Array.isArray(result[0]["a.courseScoresPerTerm"]));
    assert.isTrue(Array.isArray(result[0]["a.courseScoresPerTerm"][0]));
    assert.isTrue(Array.isArray(result[0]["a.courseScoresPerTerm"][1]));
    assert.equal(result[0]["a.courseScoresPerTerm"].length, 2);
    assert.equal(result[0]["a.courseScoresPerTerm"][0].length, 2);
    assert.equal(result[0]["a.courseScoresPerTerm"][1].length, 3);
    assert.deepEqual(result[0]["a.courseScoresPerTerm"][0][(10, 8)]);
    assert.deepEqual(result[0]["a.courseScoresPerTerm"][1][(6, 7, 8)]);
    await queryResult.close();
  });
});

describe("ARRAY", function () {
  it("should convert ARRAY type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) -[m:meets]-> (b:person) WHERE a.ID = 0 AND b.ID = 2 RETURN m.location"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("m.location" in result[0]);
    assert.isTrue(Array.isArray(result[0]["m.location"]));
    assert.equal(result[0]["m.location"].length, 2);
    assert.approximately(result[0]["m.location"][0], 7.82, EPSILON);
    assert.approximately(result[0]["m.location"][1], 3.54, EPSILON);
    await queryResult.close();
  });
});

describe("STRUCT", function () {
  it("should convert STRUCT type", async function () {
    const queryResult = await conn.query(
      "MATCH (o:organisation) WHERE o.ID = 1 RETURN o.state;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("o.state" in result[0]);
    assert.equal(typeof result[0]["o.state"], "object");
    assert.equal(result[0]["o.state"]["revenue"], 138);
    assert.deepEqual(result[0]["o.state"]["location"], ["'toronto'", "'montr,eal'"]);
    assert.equal(result[0]["o.state"]["stock"]["price"].length, 2);
    assert.equal(result[0]["o.state"]["stock"]["price"][0], 96);
    assert.equal(result[0]["o.state"]["stock"]["price"][1], 56);
    assert.equal(result[0]["o.state"]["stock"]["volume"], 1000);
    await queryResult.close();
  });
});

describe("NODE", function () {
  it("should convert NODE type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a;"
    );
    let result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a" in result[0]);
    result = result[0]["a"];
    assert.equal(result["_label"], "person");
    assert.equal(result["ID"], 0);
    assert.equal(result["gender"], 1);
    assert.isTrue(result["isStudent"]);
    assert.equal(result["age"], 35);
    assert.approximately(result["eyeSight"], 5.0, EPSILON);
    assert.equal(result["fName"], "Alice");
    assert.equal(
      Number(result["birthdate"].getTime()),
      Number(new Date("1900-01-01"))
    );
    assert.equal(
      Number(result["registerTime"]),
      Number(new Date("2011-08-20 11:25:30Z"))
    );
    assert.equal(
      result["lastJobDuration"],
      /* 3 years 2 days 13 hours 2 minutes */
      (((3 * 12 * 30 + 2) * 24 + 13) * 60 + 2) * 60 * 1000
    );
    assert.equal(result["courseScoresPerTerm"][0].length, 2);
    assert.equal(result["courseScoresPerTerm"][1].length, 3);
    assert.equal(result["courseScoresPerTerm"][0][0], 10);
    assert.equal(result["courseScoresPerTerm"][0][1], 8);
    assert.equal(result["courseScoresPerTerm"][1][0], 6);
    assert.equal(result["courseScoresPerTerm"][1][1], 7);
    assert.equal(result["courseScoresPerTerm"][1][2], 8);
    assert.equal(result["usedNames"], "Aida");
    assert.equal(result["_id"]["offset"], 0);
    assert.equal(result["_id"]["table"], 0);
    await queryResult.close();
  });
});

describe("REL", function () {
  it("should convert REL type", async function () {
    const queryResult = await conn.query(
      "MATCH (p:person)-[r:workAt]->(o:organisation) WHERE p.ID = 5 RETURN p, r, o"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.exists(result[0]["p"]);
    assert.exists(result[0]["r"]);
    assert.exists(result[0]["o"]);
    const p = result[0]["p"];
    const rel = result[0]["r"];
    const o = result[0]["o"];
    assert.deepEqual(p._id, rel._src);
    assert.deepEqual(o._id, rel._dst);
    assert.equal(rel.year, 2010);
    assert.equal(rel.grading.length, 2);
    assert.equal(rel.grading[0], 2.1);
    assert.equal(rel.grading[1], 4.4);
    assert.equal(rel._label, "workAt");
    assert.approximately(rel.rating, 7.6, EPSILON);
    assert.exists(rel._id);
    assert.exists(rel._id.offset);
    assert.exists(rel._id.table);
    await queryResult.close();
  });
});

describe("RECURSIVE_REL", function () {
  it("should convert RECURSIVE_REL type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person)-[e:studyAt*1..1]->(b:organisation) WHERE a.fName = 'Alice' RETURN e;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.exists(result[0]["e"]);
    const e = result[0]["e"];
    assert.deepEqual(e._nodes, []);
    assert.equal(e._rels.length, 1);
    assert.equal(e._rels[0].year, 2021);
    assert.deepEqual(e._rels[0].places, ["wwAewsdndweusd", "wek"]);
    assert.equal(e._rels[0].length, 5);
    assert.equal(e._rels[0].level, 5);
    assert.equal(e._rels[0].code, 9223372036854776000);
    assert.equal(e._rels[0].temperature, 32800);
    assert.equal(e._rels[0].ulength, 33768);
    assert.equal(e._rels[0].ulevel, 250);
    assert.equal(e._rels[0].hugedata, BigInt("1844674407370955161811111111"));
    assert.equal(e._rels[0]._label, "studyAt");
    assert.equal(e._rels[0]._src.offset, 0);
    assert.equal(e._rels[0]._src.table, 0);
    assert.equal(e._rels[0]._dst.offset, 0);
    assert.equal(e._rels[0]._dst.table, 1);
    assert.exists(e._rels[0]._id);
    assert.exists(e._rels[0]._id.offset);
    assert.exists(e._rels[0]._id.table);
    await queryResult.close();
  });
});

describe("MAP", function () {
  it("should convert MAP type", async function () {
    const queryResult = await conn.query(
      "MATCH (m:movies) WHERE m.length = 2544 RETURN m.audience;"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("m.audience" in result[0]);
    assert.equal(typeof result[0]["m.audience"], "object");
    assert.equal(Object.keys(result[0]["m.audience"]).length, 1);
    assert.equal(result[0]["m.audience"]["audience1"], 33);
    await queryResult.close();
  });
});

describe("DECIMAL", function () {
  it("should convert DECIMAL type", async function () {
    const queryResult = await conn.query(
      "UNWIND[1, 2, 3] AS A UNWIND[5.7, 8.3, 2.9] AS B RETURN CAST(CAST(A AS DECIMAL) * CAST(B AS DECIMAL) AS DECIMAL(18, 1)) AS PROD"
    );
    const result = await queryResult.getAllObjects();
    assert.equal(result.length, 9);
    const resultSorted = result.map(x => x.PROD).sort((a, b) => a - b);
    assert.approximately(resultSorted[0], 2.9, EPSILON);
    assert.approximately(resultSorted[1], 5.7, EPSILON);
    assert.approximately(resultSorted[2], 5.8, EPSILON);
    assert.approximately(resultSorted[3], 8.3, EPSILON);
    assert.approximately(resultSorted[4], 8.7, EPSILON);
    assert.approximately(resultSorted[5], 11.4, EPSILON);
    assert.approximately(resultSorted[6], 16.6, EPSILON);
    assert.approximately(resultSorted[7], 17.1, EPSILON);
    assert.approximately(resultSorted[8], 24.9, EPSILON);
    await queryResult.close();
  });
});
