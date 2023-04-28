const { assert } = require("chai");

describe("GETCOLUMNNAMES", function () {
  it("should return columnName header names", function () {
    conn.execute("MATCH (a:person)-[e:knows]->(b:person) RETURN a.fName, e.date, b.ID;")
        .then((queryResult) => {
            const columnNames = queryResult.getColumnNames()
            assert.equal(columnNames[0] === 'a.fName', true)
            assert.equal(columnNames[1] === 'e.date', true)
            assert.equal(columnNames[2] === 'b.ID', true)
        });
  });
});

describe("GETCOLUMNDATATYPES", function () {
    it("should return columnDataType header types", function () {
        conn.execute("MATCH (p:person) RETURN p.ID, p.fName, p.isStudent, p.eyeSight, p.birthdate, p.registerTime, " +
            "p.lastJobDuration, p.workedHours, p.courseScoresPerTerm;")
            .then((queryResult) => {
                const columnDataTypes = queryResult.getColumnDataTypes()
                const ansArr = ['INT64', 'STRING', 'BOOL', 'DOUBLE', 'DATE', 'TIMESTAMP', 'INTERVAL', 'INT64[]', 'INT64[][]']
                ansArr.forEach(function (col, i) {
                    assert.equal(columnDataTypes[i] === col, true)
                });
            });
    });
});