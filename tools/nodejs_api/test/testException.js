const { assert } = require("chai");

describe("DATABASE CONSTRUCTOR", function () {
    it('should create database with no bufferManagerSize argument', async function () {
        const testDb = new kuzu.Database(dbPath);
        const testConn = new kuzu.Connection(testDb);
        testConn.execute(`CREATE NODE TABLE testSmallPerson (ID INT64, PRIMARY KEY (ID))`).then(queryResult => {
            queryResult.all().then(result => {
                assert.equal(result.length, 1)
                assert.equal(result[0]['outputMsg'] === "NodeTable: testSmallPerson has been created.", true)
            })
        })
    });

    it('should throw error when dbPath is the wrong type', async function () {
        try {
            new kuzu.Database(5);
        }
        catch (err) {
            assert.equal(err.message, "Database constructor requires a databasePath string and optional bufferManagerSize integer as argument(s)")
        }
    });

    it('should throw error when bufferManagerSize is the wrong type', async function () {
        try {
            new kuzu.Database(dbPath, 4.5);
        }
        catch (err) {
            assert.equal(err.message, "Database constructor requires a databasePath string and optional bufferManagerSize integer as argument(s)")
        }
    });
});

describe("RESIZEBUFFERMANAGER", function () {
    it('should throw error when bufferManagerSize is decimal', async function () {
        try {
            db.resizeBufferManager(4.4)
        }
        catch (err) {
            assert.equal(err.message, "resizeBufferManager needs an integer bufferManagerSize as an argument")
        }
    });

    it('should throw error when bufferManagerSize decreases', async function () {
        try {
            db.resizeBufferManager(1)
        }
        catch (err) {
            assert.equal(err.message, "Unsuccessful resizeBufferManager: Buffer manager exception: Resizing to a smaller Buffer Pool Size is unsupported.")
        }
    });

    it('should execute successfully',  function () {
            db.resizeBufferManager(1 << 29)
    });
});

describe("CONNECTION CONSTRUCTOR", function () {
    it('should create connection with no numThreads argument', async function () {
        const testConn = new kuzu.Connection(db);
        testConn.execute(`CREATE NODE TABLE testSmallPerson (ID INT64, PRIMARY KEY (ID))`).then(queryResult => {
            queryResult.all().then(result => {
                assert.equal(result.length, 1)
                assert.equal(result[0]['outputMsg'] === "NodeTable: testSmallPerson has been created.", true)
            })
        })
    });

    it('should throw error when database object is invalid', async function () {
        try {
            new kuzu.Connection("db");
        }
        catch (err) {
            assert.equal(err.message, "Connection constructor requires a database object and optional numThreads integer as argument(s)")
        }
    });

    it('should throw error when numThreads is double', async function () {
        try {
            new kuzu.Connection(db, 5.2);
        }
        catch (err) {
            assert.equal(err.message, "Connection constructor requires a database object and optional numThreads integer as argument(s)")
        }
    });
});

describe("EXECUTE", function () {
    it('query opts should be object (map)', async function () {
        try {
            const prom = await conn.execute("MATCH (a:person) RETURN a.isStudent", "");
        } catch (err) {
            assert.equal(err.message, "optional opts in execute must be an object");
        }
    });

    it('query opts too many fields', async function () {
        try {
            const prom = await conn.execute("MATCH (a:person) RETURN a.isStudent", {'james': {}, 'hi': {}, 'bye': {}});
        } catch (err) {
            assert.equal(err.message, "opts can only have optional fields 'callback' and/or 'params'");
        }
    });

    it('query opts invalid field', async function () {
        try {
            const prom = await conn.execute("MATCH (a:person) RETURN a.isStudent", {'james': {}});
        } catch (err) {
            assert.equal(err.message, "opts has at least 1 invalid field: it can only have optional fields 'callback' and/or 'params'");
        }
    });

    it('query field should be string', async function () {
        try {
            await conn.execute(5);
        } catch (err) {
            assert.equal(err.message, "execute takes a string query and optional parameter array");
        }
    });

    it('query param in opts should be array', async function () {
        try {
            await conn.execute("MATCH (a:person) RETURN a.isStudent", {"params": {}});
        } catch (err) {
            assert.equal(err.message, "execute takes a string query and optional parameter array");
        }
    });

    it('query callback in opts should be function', async function () {
        try {
            await conn.execute("MATCH (a:person) RETURN a.isStudent", {"callback": ""});
        } catch (err) {
            assert.equal(err.message, "if execute is given a callback, it must take 2 arguments: (err, result)");
        }
    });

    it('query callback in opts should be function that takes 2 arguments', async function () {
        try {
            await conn.execute("MATCH (a:person) RETURN a.isStudent", {"callback": function(a){}});
        } catch (err) {
            assert.equal(err.message, "if execute is given a callback, it must take 2 arguments: (err, result)");
        }
    });

    it('should throw error when query refers to non-existing field', async function (done) {
        try {
            conn.execute("MATCH (a:person) RETURN a.dummy;").catch(err => {
                throw new Error(err);
            })
        } catch (err) {
            assert.equal(err.message, "Binder exception: Cannot find property dummy for a.");
        }
        done()
    });
});

describe("SETMAXTHREADS", function () {
    it('decrease max threads', function () {
        conn.setMaxNumThreadForExec(2);
    });

    it('increase max threads', function () {
        conn.setMaxNumThreadForExec(5);
    });

    it('should only take number as argument', function () {
        try {
            conn.setMaxNumThreadForExec("hi");
        } catch (err) {
            assert.equal(err.message, "setMaxNumThreadForExec needs an integer numThreads as an argument");;
        }
    });

    it('should only take integer as argument', function () {
        try {
            conn.setMaxNumThreadForExec(5.6);
        } catch (err) {
            assert.equal(err.message, "setMaxNumThreadForExec needs an integer numThreads as an argument");;
        }
    });
});

describe("GETNODEPROPERTYNAMES", function () {
    it('should return valid property names for valid tableName', function () {
        const propertyNames = conn.getNodePropertyNames("organisation");
        assert.equal(typeof propertyNames, "string");
        assert.equal(propertyNames.length>0, true);
    });

    it('should throw error for non string argument', function () {
        try {
            conn.getNodePropertyNames(2);
        } catch (err) {
            assert.equal(err.message, "getNodePropertyNames needs a string tableName as an argument");
        }
    });
});


describe("QUERYRESULT CONSTRUCTOR", function () {
    it('should throw error when invalid queryresult passed in',  function () {
        try {
            new kuzu.QueryResult(conn, {});
        } catch (err) {
            assert.equal(err.message, "QueryResult constructor requires a 'NodeQueryResult' object as an argument");
        }
    });

    it('should throw error when invalid connection passed in',  function () {
        try {
            new kuzu.QueryResult({}, {});
        } catch (err) {
            assert.equal(err.message, "Connection constructor requires a 'Connection' object as an argument");
        }
    });
});

describe("EACH", function () {
    it('should throw error when rowCallback to each is invalid', function (done) {
            conn.execute("MATCH (a:person) RETURN a.isStudent").then(queryResult => {
                queryResult.each((err) => {
                    console.log(err);
                }, () => console.log("done")).catch(err => {
                    assert.equal(err.message, "all must have 2 callbacks: rowCallback takes 2 arguments: (err, result), doneCallback takes none");
                    done();
                });
            })
    })

    it('should throw error when doneCallback to each is invalid', function (done) {
        conn.execute("MATCH (a:person) RETURN a.isStudent").then(queryResult => {
            queryResult.each((err, result) => {
                console.log(result);
            }, (err) => console.log(err)).catch(err => {
                assert.equal(err.message, "all must have 2 callbacks: rowCallback takes 2 arguments: (err, result), doneCallback takes none");
                done();
            });
        })
    })

});

describe("ALL", function () {
    it('should throw error when invalid filed in opts', function (done) {
        conn.execute("MATCH (a:person) RETURN a.isStudent").then(queryResult => {
            queryResult.all({'james': {}}).catch(err => {
                assert.equal(err.message, "opts has at least 1 invalid field: it can only have optional field 'callback'");
                done();
            });
        })
    })

    it('should throw error when callback has invalid params', function (done) {
        conn.execute("MATCH (a:person) RETURN a.isStudent").then(queryResult => {
            queryResult.all({'callback': {function(err) { console.log(err) }}}).catch(err => {
                assert.equal(err.message, "if execute is given a callback, it must take 2 arguments: (err, result)");
                done();
            });
        })
    })

    it('should throw error when callback is invalid type', function (done) {
        conn.execute("MATCH (a:person) RETURN a.isStudent").then(queryResult => {
            queryResult.all({'callback': 5}).catch(err => {
                assert.equal(err.message, "if execute is given a callback, it must take 2 arguments: (err, result)");
                done();
            });
        })
    })

    it('should execute successfully with callback', function (done) {
        conn.execute("MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;").then((queryResult) => {
            queryResult.all({
                'callback': function (err, result) {
                    assert.equal(result[0]["a.isStudent"], true);
                    done();
                }
            });
        });
    })
});