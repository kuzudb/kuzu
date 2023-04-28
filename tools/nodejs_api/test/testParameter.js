const { assert } = require("chai");

describe("TYPES", function () {
  it("should return successfully for bool param to execute", function (done) {
        conn.execute("MATCH (a:person) WHERE a.isStudent = $1 AND a.isWorker = $k RETURN COUNT(*)",
            {'params': [["1", false], ["k", false]]})
            .then((queryResult) => {
                queryResult.all().then(result => {
                    assert.equal(result[0]['COUNT_STAR()'], 1)
                    done();
                })
            });
    });

    it("should return successfully for int param to execute", function (done) {
        conn.execute("MATCH (a:person) WHERE a.age < $AGE RETURN COUNT(*)",
            {'params': [["AGE", 1]]} )
            .then((queryResult) => {
                queryResult.all().then(result => {
                    assert.equal(result[0]['COUNT_STAR()'], 0)
                    done();
                })
            });
    });

    // TODO: When the param is 5.0 it will think it's a INT64 since it can be parsed into an INT,
    //  however 5.1, etc., correctly parses into a DOUBLE
    // it("should return successfully for double param to execute", function (done) {
    //     conn.execute("MATCH (a:person) WHERE a.eyeSight = $E RETURN COUNT(*)",
    //         {'params': [["E", 5.0]]})
    //         .then((queryResult) => {
    //             queryResult.all().then(result => {
    //                 assert.equal(result[0]['COUNT_STAR()'], 2)
    //                 done();
    //             })
    //         }).catch(err => { console.log(err); done();});
    // });

    it("should return successfully for string param to execute", function (done) {
        conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN concat(a.fName, $S);",
            {'params': [["S", "HH"]]})
            .then((queryResult) => {
                queryResult.all().then(result => {
                    assert.equal(result[0]['CONCAT(a.fName,$S)'], "AliceHH")
                    done();
                })
            });
    });

    it("should return successfully for date param to execute", function (done) {
        conn.execute("MATCH (a:person) WHERE a.birthdate = $1 RETURN COUNT(*);",
            {'params': [["1",  new Date(1900,0,1,-5)]]})
            .then((queryResult) => {
                queryResult.all().then(result => {
                    assert.equal(result[0]['COUNT_STAR()'], 2)
                    done();
                })
            });
    });

    it("should return successfully for timestamp param to execute", function (done) {
        conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);",
            {'params': [["1", new Date(2011,7,20,7,25,30)]]})
            .then((queryResult) => {
                queryResult.all().then(result => {
                    assert.equal(result[0]['COUNT_STAR()'], 1)
                    done();
                })
            });
    });
});


describe("EXCEPTION", function () {
    it("should throw error when invalid name type is passed", function () {
        return new Promise(async function (resolve) {
            try {
                await conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);",
                    {'params': [[1, 1]]})
            } catch (err) {
                assert.equal(err.message, "Unsuccessful execute: Parameter name must be of type string")
                resolve();
            }
        })
    });

    it("should throw error when param not tuple", async function () {
        return new Promise(async function (resolve) {
            try {
                await conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);",
                    {'params': [["asd"]]})
            } catch (err) {
                assert.equal(err.message, "Unsuccessful execute: Each parameter must be in the form of <name, val>")
                resolve();
            }
        })
    });

    it("should throw error when param not tuple (long)", async function () {
        return new Promise(async function (resolve) {
            try {
                await conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);",
                    {'params': [["asd", 1, 1]]})
            } catch (err) {
                assert.equal(err.message, "Unsuccessful execute: Each parameter must be in the form of <name, val>")
                resolve();
            }
        })
    });

    it("should throw error when param value is invalid type", async function () {
        return new Promise(async function (resolve) {
            try {
                await conn.execute("MATCH (a:person) WHERE a.registerTime = $1 RETURN COUNT(*);",
                    {'params': [["asd", {}]]})
            } catch (err) {
                assert.equal(err.message, "Unsuccessful execute: Unknown parameter type")
                resolve();
            }
        })
    });
});
