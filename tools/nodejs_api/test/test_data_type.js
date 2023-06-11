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
      REVENUE: 138,
      LOCATION: ["'toronto'", " 'montr,eal'"],
      STOCK: { PRICE: [96, 56], VOLUME: 1000 },
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
    assert.approximately(rel.rating, 7.6, EPSILON);
  });
});
