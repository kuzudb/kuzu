const { assert } = require("chai");
const EPSILON = 1e-6;

describe("BOOL", function () {
  it("should convert BOOL type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.isStudent" in result[0]);
    assert.equal(typeof result[0]["a.isStudent"], "boolean");
    assert.isTrue(result[0]["a.isStudent"]);
  });
});

describe("INT8", function () {
  it("should convert INT8 type", async function () {
    const queryResult = await conn.query(
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE a.ID = 0 RETURN s.level"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("s.level" in result[0]);
    assert.equal(typeof result[0]["s.level"], "number");
    assert.equal(result[0]["s.level"], 5);
  });
});

describe("INT16", function () {
  it("should convert INT16 type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE a.ID = 0 RETURN s.length"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("s.length" in result[0]);
    assert.equal(typeof result[0]["s.length"], "number");
    assert.equal(result[0]["s.length"], 5);
  });
});

describe("INT32", function () {
  it("should convert INT32 type", async function () {
    const queryResult = await conn.query(
      'MATCH (a:movies) WHERE a.name = "Roma" RETURN a.length'
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.length" in result[0]);
    assert.equal(typeof result[0]["a.length"], "number");
    assert.equal(result[0]["a.length"], 298);
  });
});

describe("INT64", function () {
  it("should convert INT64 type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.ID;"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.ID" in result[0]);
    assert.equal(typeof result[0]["a.ID"], "number");
    assert.equal(result[0]["a.ID"], 0);
  });
});

describe("UINT8", function () {
  it("should convert UINT8 type", async function () {
    const queryResult = await conn.query(
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE a.ID = 0 RETURN s.ulevel"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("s.ulevel" in result[0]);
    assert.equal(typeof result[0]["s.ulevel"], "number");
    assert.equal(result[0]["s.ulevel"], 250);
  });
});

describe("UINT16", function () {
  it("should convert UINT16 type", async function () {
    const queryResult = await conn.query(
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE a.ID = 0 RETURN s.ulength"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("s.ulength" in result[0]);
    assert.equal(typeof result[0]["s.ulength"], "number");
    assert.equal(result[0]["s.ulength"], 33768);
  });
});

describe("UINT32", function () {
  it("should convert UINT32 type", async function () {
    const queryResult = await conn.query(
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE a.ID = 0 RETURN s.temprature"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("s.temprature" in result[0]);
    assert.equal(typeof result[0]["s.temprature"], "number");
    assert.equal(result[0]["s.temprature"], 32800);
  });
});

describe("UINT64", function () {
  it("should convert UINT64 type", async function () {
    const queryResult = await conn.query(
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE a.ID = 0 RETURN s.code"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("s.code" in result[0]);
    assert.equal(typeof result[0]["s.code"], "number");
    assert.equal(result[0]["s.code"], 9223372036854775808);
  });
});

describe("SERIAL", function () {
  it("should convert SERIAL type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:moviesSerial) WHERE a.ID = 2 RETURN a.ID;"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.ID" in result[0]);
    assert.equal(typeof result[0]["a.ID"], "number");
    assert.equal(result[0]["a.ID"], 2);
  });
});

describe("FLOAT", function () {
  it("should convert FLOAT type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.height;"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.height" in result[0]);
    assert.equal(typeof result[0]["a.height"], "number");
    assert.approximately(result[0]["a.height"], 1.731, EPSILON);
  });
});

describe("DOUBLE", function () {
  it("should convert DOUBLE type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.eyeSight;"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.eyeSight" in result[0]);
    assert.equal(typeof result[0]["a.eyeSight"], "number");
    assert.approximately(result[0]["a.eyeSight"], 5.0, EPSILON);
  });
});

describe("STRING", function () {
  it("should convert STRING type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.fName;"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.fName" in result[0]);
    assert.equal(typeof result[0]["a.fName"], "string");
    assert.equal(result[0]["a.fName"], "Alice");
  });
});

describe("BLOB", function () {
  it("should convert BLOB type", async function () {
    const queryResult = await conn.query(
      "RETURN BLOB('\\\\xAA\\\\xBB\\\\xCD\\\\x1A');"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    const value = Object.values(result[0])[0];
    assert.isTrue(value instanceof Uint8Array);
    assert.equal(value.length, 4);
    assert.equal(value[0], 0xaa);
    assert.equal(value[1], 0xbb);
    assert.equal(value[2], 0xcd);
    assert.equal(value[3], 0x1a);
  });
});

describe("DATE", function () {
  it("should convert DATE type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.birthdate;"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.birthdate" in result[0]);
    assert.equal(typeof result[0]["a.birthdate"], "object");
    assert.equal(
      Number(result[0]["a.birthdate"]),
      Number(new Date("1900-01-01"))
    );
  });
});

describe("TIMESTAMP", function () {
  it("should convert TIMESTAMP type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.registerTime;"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.registerTime" in result[0]);
    assert.equal(typeof result[0]["a.registerTime"], "object");
    assert.equal(
      Number(result[0]["a.registerTime"].getTime()),
      Number(new Date("2011-08-20 11:25:30Z"))
    );
  });
});

describe("INTERVAL", function () {
  it("should convert INTERVAL type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.lastJobDuration;"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("a.lastJobDuration" in result[0]);
    assert.equal(typeof result[0]["a.lastJobDuration"], "number");
    assert.equal(
      result[0]["a.lastJobDuration"],
      /* 3 years 2 days 13 hours 2 minutes */
      (((3 * 12 * 30 + 2) * 24 + 13) * 60 + 2) * 60 * 1000
    );
  });
});

describe("VAR_LIST", function () {
  it("should convert VAR_LIST type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a.courseScoresPerTerm;"
    );
    const result = await queryResult.getAll();
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
  });
});

describe("FIXED_LIST", function () {
  it("should convert FIXED_LIST type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) -[m:meets]-> (b:person) WHERE a.ID = 0 AND b.ID = 2 RETURN m.location"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("m.location" in result[0]);
    assert.isTrue(Array.isArray(result[0]["m.location"]));
    assert.equal(result[0]["m.location"].length, 2);
    assert.approximately(result[0]["m.location"][0], 7.82, EPSILON);
    assert.approximately(result[0]["m.location"][1], 3.54, EPSILON);
  });
});

describe("STRUCT", function () {
  it("should convert STRUCT type", async function () {
    const queryResult = await conn.query(
      "MATCH (o:organisation) WHERE o.ID = 1 RETURN o.state;"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("o.state" in result[0]);
    assert.equal(typeof result[0]["o.state"], "object");
    assert.deepEqual(result[0]["o.state"], {
      revenue: 138,
      location: ["'toronto'", " 'montr,eal'"],
      stock: { price: [96, 56], volume: 1000 },
    });
  });
});

describe("NODE", function () {
  it("should convert NODE type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) WHERE a.ID = 0 RETURN a;"
    );
    let result = await queryResult.getAll();
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
    assert.deepEqual(result["courseScoresPerTerm"][0], [10, 8]);
    assert.deepEqual(result["courseScoresPerTerm"][1], [6, 7, 8]);
    assert.equal(result["usedNames"], "Aida");
    assert.equal(result["_id"]["offset"], 0);
    assert.equal(result["_id"]["table"], 0);
  });
});

describe("REL", function () {
  it("should convert REL type", async function () {
    const queryResult = await conn.query(
      "MATCH (p:person)-[r:workAt]->(o:organisation) WHERE p.ID = 5 RETURN p, r, o"
    );
    const result = await queryResult.getAll();
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
  });
});

describe("RECURSIVE_REL", function () {
  it("should convert RECURSIVE_REL type", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person)-[e*1..1]->(b:organisation) WHERE a.fName = 'Alice' RETURN e;"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.exists(result[0]["e"]);
    const e = result[0]["e"];
    assert.deepEqual(e, {
      _nodes: [],
      _rels: [
        {
          date: null,
          meetTime: null,
          validInterval: null,
          comments: null,
          year: 2021,
          places: ["wwAewsdndweusd", "wek"],
          length: 5,
          level: 5,
          code: 9223372036854776000,
          temprature: 32800,
          ulength: 33768,
          ulevel: 250,
          hugedata: BigInt("1844674407370955161811111111"),
          grading: null,
          rating: null,
          location: null,
          times: null,
          data: null,
          usedAddress: null,
          address: null,
          note: null,
          notes: null,
          summary: null,
          _label: "studyAt",
          _src: {
            offset: 0,
            table: 0,
          },
          _dst: {
            offset: 0,
            table: 1,
          },
        },
      ],
    });
  });
});

describe("MAP", function () {
  it("should convert MAP type", async function () {
    const queryResult = await conn.query(
      "MATCH (m:movies) WHERE m.length = 2544 RETURN m.audience;"
    );
    const result = await queryResult.getAll();
    assert.equal(result.length, 1);
    assert.equal(Object.keys(result[0]).length, 1);
    assert.isTrue("m.audience" in result[0]);
    assert.equal(typeof result[0]["m.audience"], "object");
    assert.deepEqual(result[0]["m.audience"], {'audience1': 33});
  });
});
