const { assert } = require("chai");

const PERSON_IDS = [0, 2, 3, 5, 7, 8, 9, 10];

describe("Reset iterator", function () {
  it("should reset the iterator of the query result", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) RETURN a.ID ORDER BY a.ID"
    );
    assert.isTrue(queryResult.hasNext());
    let tuple = await queryResult.getNext();
    assert.equal(tuple[0], 0);
    tuple = await queryResult.getNext();
    assert.equal(tuple[0], 2);
    tuple = await queryResult.getNext();
    assert.equal(tuple[0], 3);
    queryResult.resetIterator();
    tuple = await queryResult.getNext();
    assert.equal(tuple[0], 0);
    tuple = await queryResult.getNext();
    assert.equal(tuple[0], 2);
    tuple = await queryResult.getNext();
    assert.equal(tuple[0], 3);
    await queryResult.close();
  });
});

describe("Has next", function () {
  it("should return true if there is a next tuple", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) RETURN a.ID ORDER BY a.ID"
    );
    for (let i = 0; i < 8; ++i) {
      assert.isTrue(queryResult.hasNext());
      await queryResult.getNext();
    }
    assert.isFalse(queryResult.hasNext());
    await queryResult.close();
  });

  it("should return false if there is no next tuple", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) RETURN a.ID ORDER BY a.ID"
    );
    for (let i = 0; i < 8; ++i) {
      await queryResult.getNext();
    }
    assert.isFalse(queryResult.hasNext());
    await queryResult.close();
  });
});

describe("Get number of tuples", function () {
  it("should return the number of tuples", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) RETURN a.ID ORDER BY a.ID"
    );
    assert.equal((await queryResult.getNumTuples()), 8);
    await queryResult.close();
  });
});

describe("Get number of columns", function () {
  it("should return the number of columns", async function () {
    const queryResult = await conn.query(
      "RETURN 1, 2, 3, 4, 5, 6, 7, 8"
    );
    assert.equal((await queryResult.getNumColumns()), 8);
    await queryResult.close();
  });
});

describe("Get next", function () {
  it("should return the next tuple", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) RETURN a.ID ORDER BY a.ID"
    );
    for (let i = 0; i < 8; ++i) {
      const tuple = await queryResult.getNext();
      assert.equal(tuple[0], PERSON_IDS[i]);
    }
  });
});

describe("Get all as objects", function () {
  it("should return all tuples in order", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) RETURN a.ID ORDER BY a.ID"
    );
    const tuples = await queryResult.getAllObjects();
    const ids = tuples.map((tuple) => tuple["a.ID"]);
    for (let i = 0; i < 8; ++i) {
      assert.equal(ids[i], PERSON_IDS[i]);
    }
    await queryResult.close();
  });
});

describe("Get all as arrays", function () {
  it("should return all tuples in order", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) RETURN a.ID ORDER BY a.ID"
    );
    const tuples = await queryResult.getAllRows();
    const ids = tuples.map((tuple) => tuple[0]);
    for (let i = 0; i < 8; ++i) {
      assert.equal(ids[i], PERSON_IDS[i]);
    }
    await queryResult.close();
  });
});

describe("Get column names", function () {
  it("should return column names", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person)-[e:knows]->(b:person) RETURN a.fName, e.date, b.ID;"
    );
    const columnNames = await queryResult.getColumnNames();
    const expectedResult = ["a.fName", "e.date", "b.ID"];
    assert.deepEqual(columnNames, expectedResult);
  });
});

describe("Get column data types", function () {
  it("should return column data types", async function () {
    const queryResult = await conn.query(
      `MATCH (p:person) 
         RETURN p.ID, p.fName, p.isStudent, p.eyeSight, p.birthdate, 
                p.registerTime, p.lastJobDuration, p.workedHours, 
                p.courseScoresPerTerm`
    );
    const columnDataTypes = await queryResult.getColumnTypes();
    const ansexpectedResultArr = [
      "INT64",
      "STRING",
      "BOOL",
      "DOUBLE",
      "DATE",
      "TIMESTAMP",
      "INTERVAL",
      "INT64[]",
      "INT64[][]",
    ];
    assert.deepEqual(columnDataTypes, ansexpectedResultArr);
  });
});

describe("Close", function () {
  it("should close the query result", async function () {
    const queryResult = await conn.query(
      "MATCH (a:person) RETURN a.ID ORDER BY a.ID"
    );
    assert.isFalse(queryResult._isClosed);
    await queryResult.close();
    assert.isTrue(queryResult._isClosed);
    try {
      await queryResult.getNext();
      assert.fail("No error thrown when the query result is closed");
    } catch (err) {
      assert.equal(err.message, "Query result not found");
    }
  });
});
