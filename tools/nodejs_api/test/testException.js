const { assert } = require("chai");
describe("EXECUTE", function () {
    it('query type', async function (done) {
        try {
            await conn.execute(5);
        } catch (err) {
            assert.equal(err.message, "execute takes a string query and optional parameter array");
            done();
        }
    });

    it('query opts too many fields', async function (done) {
        try {
            const prom = await conn.execute("MATCH (a:person) RETURN a.isStudent", {'james': {}, 'hi': {}, 'bye': {}});
        } catch (err) {
            assert.equal(err.message, "opts can only have optional fields 'callback' and/or 'params'");
        }
        done();
    });

    it('query param in opts is array', async function (done) {
        try {
            await conn.execute("MATCH (a:person) RETURN a.isStudent", {"params": {}});
        } catch (err) {
            assert.equal(err.message, "execute takes a string query and optional parameter array");
            done();
        }
    });


});