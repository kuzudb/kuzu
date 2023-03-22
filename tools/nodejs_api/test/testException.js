const { assert } = require("chai");
describe("EXECUTE", function () {
    it('query opts should be object (map)', async function (done) {
        try {
            const prom = await conn.execute("MATCH (a:person) RETURN a.isStudent", "");
        } catch (err) {
            assert.equal(err.message, "optional opts in execute must be an object");
        }
        done();
    });

    it('query opts too many fields', async function (done) {
        try {
            const prom = await conn.execute("MATCH (a:person) RETURN a.isStudent", {'james': {}, 'hi': {}, 'bye': {}});
        } catch (err) {
            assert.equal(err.message, "opts can only have optional fields 'callback' and/or 'params'");
        }
        done();
    });

    it('query opts invalid field', async function (done) {
        try {
            const prom = await conn.execute("MATCH (a:person) RETURN a.isStudent", {'james': {}});
        } catch (err) {
            assert.equal(err.message, "opts has at least 1 invalid field: it can only have optional fields 'callback' and/or 'params'");
        }
        done();
    });

    it('query field should be string', async function (done) {
        try {
            await conn.execute(5);
        } catch (err) {
            assert.equal(err.message, "execute takes a string query and optional parameter array");
            done();
        }
    });

    it('query param in opts should be array', async function (done) {
        try {
            await conn.execute("MATCH (a:person) RETURN a.isStudent", {"params": {}});
        } catch (err) {
            assert.equal(err.message, "execute takes a string query and optional parameter array");
            done();
        }
    });

    it('query callback in opts should be function', async function (done) {
        try {
            await conn.execute("MATCH (a:person) RETURN a.isStudent", {"callback": ""});
        } catch (err) {
            assert.equal(err.message, "if execute is given a callback, it must take 2 arguments: (err, result)");
            done();
        }
    });

    it('query callback in opts should be function that takes 2 arguments', async function (done) {
        try {
            await conn.execute("MATCH (a:person) RETURN a.isStudent", {"callback": function(a){}});
        } catch (err) {
            assert.equal(err.message, "if execute is given a callback, it must take 2 arguments: (err, result)");
            done();
        }
    });
});

describe("SETMAXTHREADS", function () {
    it('query callback in opts should be function that takes 2 arguments', function () {
        try {
            conn.setMaxNumThreadForExec(0);
        } catch (err) {
            console.log(err)
            assert.equal(err.message, "Buffer manager exception: Resizing to a smaller Buffer Pool Size is unsupported.")
        }
    });
});
