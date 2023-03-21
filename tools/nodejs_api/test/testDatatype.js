const { assert } = require("chai");

describe("BOOL", function () {
  it("should return Boolean for BOOL types as promise", function (done) {
    conn
        .execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;")
        .then((queryResult) => {
          return queryResult.all();
        })
        .then((result) => {
          assert.equal(result.length, 1);
          assert.equal(Object.keys(result[0]).length, 1)
          assert.equal("a.isStudent" in result[0], true)
          assert.equal(typeof result[0]["a.isStudent"], "boolean");
          assert.equal(result[0]["a.isStudent"], true);
          done();
        });
  });
  it("should return Boolean for BOOL types as callback", function (done) {
    conn
        .execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;", {"callback" : async function (err, queryResult) {
            assert.equal(err, null);
            await queryResult.all({
              "callback": (err, result) => {
                assert.equal(result.length, 1);
                assert.equal(Object.keys(result[0]).length, 1)
                assert.equal("a.isStudent" in result[0], true)
                assert.equal(typeof result[0]["a.isStudent"], "boolean");
                assert.equal(result[0]["a.isStudent"], true);
                done();
              }
            });
          }
        });
  });
});


describe("INT", function () {
    it("should return Integer for INT types as promise", function (done) {
        conn
        .execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.age;")
        .then((queryResult) => {
            return queryResult.all();
        })
        .then((result) => {
            assert.equal(result.length, 1);
            assert.equal(Object.keys(result[0]).length, 1)
            assert.equal("a.age" in result[0], true)
            assert.equal(typeof result[0]["a.age"], "number");
            assert.equal(result[0]["a.age"], 35);
            done();
        });
    });
});

describe("DOUBLE", function () {
    it("should return Double for DOUBLE types as promise", function (done) {
        conn
            .execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.eyeSight;")
            .then((queryResult) => {
                return queryResult.all();
            })
            .then((result) => {
                assert.equal(result.length, 1);
                assert.equal(Object.keys(result[0]).length, 1)
                assert.equal("a.eyeSight" in result[0], true)
                assert.equal(typeof result[0]["a.eyeSight"], "number");
                assert.equal(result[0]["a.eyeSight"], 5.0);
                done();
            });
    });
});

describe("STRING", function () {
    it("should return String for STRING types as promise", function (done) {
        conn
            .execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName;")
            .then((queryResult) => {
                return queryResult.all();
            })
            .then((result) => {
                assert.equal(result.length, 1);
                assert.equal(Object.keys(result[0]).length, 1)
                assert.equal("a.fName" in result[0], true)
                assert.equal(typeof result[0]["a.fName"], "string");
                assert.equal(result[0]["a.fName"], "Alice");
                done();
            });
    });
});

describe("DATE", function () {
    it("should return Date for DATE types as promise", function (done) {
        conn
            .execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.birthdate;")
            .then((queryResult) => {
                return queryResult.all();
            })
            .then((result) => {
                assert.equal(result.length, 1);
                assert.equal(Object.keys(result[0]).length, 1)
                assert.equal("a.birthdate" in result[0], true)
                assert.equal(typeof result[0]["a.birthdate"], "object");
                assert.equal(result[0]["a.birthdate"].getTime(), new Date(1900,0,1,-5).getTime());
                done();
            });
    });
});


describe("TIMESTAMP", function () {
    it("should return Timestamp for TIMESTAMP types as promise", function (done) {
        conn
            .execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.registerTime;")
            .then((queryResult) => {
                return queryResult.all();
            })
            .then((result) => {
                assert.equal(result.length, 1);
                assert.equal(Object.keys(result[0]).length, 1)
                assert.equal("a.registerTime" in result[0], true)
                assert.equal(typeof result[0]["a.registerTime"], "object");
                assert.equal(result[0]["a.registerTime"].getTime(), new Date(2011,7,20,7,25,30).getTime());
                done();
            });
    });
});


describe("INTERVAL", function () {
    it("should return Interval for INTERVAL types as promise", function (done) {
        conn
            .execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.lastJobDuration;")
            .then((queryResult) => {
                return queryResult.all();
            })
            .then((result) => {
                assert.equal(result.length, 1);
                assert.equal(Object.keys(result[0]).length, 1)
                assert.equal("a.lastJobDuration" in result[0], true)
                assert.equal(typeof result[0]["a.lastJobDuration"], "object");
                assert.equal(Object.keys(result[0]["a.lastJobDuration"]).length, 2);
                assert.equal("days" in result[0]["a.lastJobDuration"], true);
                assert.equal("microseconds" in result[0]["a.lastJobDuration"], true);
                assert.equal(typeof result[0]["a.lastJobDuration"]["days"], "number");
                assert.equal(typeof result[0]["a.lastJobDuration"]["microseconds"], "number");
                assert.equal(result[0]["a.lastJobDuration"]["days"], 1082);
                assert.equal(result[0]["a.lastJobDuration"]["microseconds"], 46920000000);
                done();
            });
    });
});

describe("LIST", function () {
    it("should return List for LIST types as promise", function (done) {
        conn
            .execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.courseScoresPerTerm;")
            .then((queryResult) => {
                return queryResult.all();
            })
            .then((result) => {
                assert.equal(result.length, 1);
                assert.equal(Object.keys(result[0]).length, 1)
                assert.equal("a.courseScoresPerTerm" in result[0], true)
                assert.equal(typeof result[0]["a.courseScoresPerTerm"], "object");
                assert.equal(result[0]["a.courseScoresPerTerm"].length, 2);
                assert.equal(result[0]["a.courseScoresPerTerm"][0].length, 2);
                assert.equal(result[0]["a.courseScoresPerTerm"][1].length, 3);
                assert.equal(JSON.stringify(result[0]["a.courseScoresPerTerm"][0]), JSON.stringify([10,8]));
                assert.equal(JSON.stringify(result[0]["a.courseScoresPerTerm"][1]), JSON.stringify([6,7,8]));
                done();
            });
    });
});

describe("NODE", function () {
    it("should return Node for NODE types as promise", function (done) {
        conn
            .execute("MATCH (a:person) WHERE a.ID = 0 RETURN a;")
            .then((queryResult) => {
                return queryResult.all();
            })
            .then((result) => {
                assert.equal(result.length, 1);
                assert.equal(Object.keys(result[0]).length, 1)
                assert.equal("a" in result[0], true)
                result = result[0]["a"]
                assert.equal(result["_label"], "person");
                assert.equal(result["ID"], 0);
                assert.equal(result["gender"], 1);
                assert.equal(result["isStudent"], true);
                assert.equal(result["age"], 35);
                assert.equal(result["eyeSight"], 5.0);
                assert.equal(result["fName"], "Alice");
                assert.equal(result["birthdate"].getTime(), new Date(1900,0,1,-5).getTime());
                assert.equal(result["registerTime"].getTime(), new Date(2011,7,20,7,25,30).getTime());
                assert.equal(result["lastJobDuration"]["days"], 1082);
                assert.equal(result["lastJobDuration"]["microseconds"], 46920000000);
                assert.equal(JSON.stringify(result["courseScoresPerTerm"][0]), JSON.stringify([10,8]));
                assert.equal(JSON.stringify(result["courseScoresPerTerm"][1]), JSON.stringify([6,7,8]));
                assert.equal(result["usedNames"], "Aida");
                assert.equal(result["_id"]["offset"], 0);
                assert.equal(result["_id"]["table"], 0);
                done();
            });
    });
});

describe("REL", function () {
    it("should return Rel for REL types as promise", function (done) {
        conn
            .execute("MATCH (p:person)-[r:workAt]->(o:organisation) WHERE p.ID = 5 RETURN p, r, o")
            .then((queryResult) => {
                return queryResult.all();
            })
            .then((n) => {
                n = n[0]
                const p = n["p"];
                const r = n["r"];
                const o = n["o"];
                assert.equal(p['_label'], 'person')
                assert.equal(p['ID'], 5)
                assert.equal(o['_label'],  'organisation')
                assert.equal(o['ID'], 6)
                assert.equal(r['year'], 2010)
                assert.equal(JSON.stringify(r['_src']), JSON.stringify(p['_id']))
                assert.equal(JSON.stringify(r['_dst']), JSON.stringify(o['_id']))
                done();
            });
    });
});
