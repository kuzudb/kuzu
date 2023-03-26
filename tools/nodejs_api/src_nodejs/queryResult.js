class QueryResult {
    #connection
    #database;
    #queryResult
    constructor(connection, queryResult) {
        if (typeof connection !== "object" || connection.constructor.name !==  "Connection"){
            throw new Error("Connection constructor requires a 'Connection' object as an argument");
        } else if (typeof queryResult !== "object" || queryResult.constructor.name !==  "NodeQueryResult"){
            throw new Error("QueryResult constructor requires a 'NodeQueryResult' object as an argument");
        }
        this.#connection = connection;
        this.#queryResult = queryResult;
    }

     async each(rowCallback, doneCallback) {
         return new Promise(async (resolve, reject) => {
             if (typeof rowCallback !== 'function' || rowCallback.length !== 2 || typeof doneCallback !== 'function' || doneCallback.length !== 0) {
                 return reject(new Error("all must have 2 callbacks: rowCallback takes 2 arguments: (err, result), doneCallback takes none"));
             }
             this.#queryResult.each(rowCallback, doneCallback).catch(err => {
                 return reject(err)
             });
         });
    }

    async all(opts = {} ) {
        return new Promise(async (resolve, reject) => {
            const optsKeys = Object.keys(opts);
            const validSet = new Set(optsKeys.concat(['callback']));
            if (validSet.size > 1) {
                return reject(new Error("opts has at least 1 invalid field: it can only have optional field 'callback'"));
            } else if ('callback' in opts) {
                const callback = opts['callback'];
                if (typeof callback !== 'function' || callback.length !== 2) {
                    return reject(new Error("if execute is given a callback, it must take 2 arguments: (err, result)"));
                }
                this.#queryResult.all(callback).catch(err => {
                    return reject(err);
                });
            } else {
                this.#queryResult.all((err, result) => {
                    if (err) {
                        return reject(err);
                    }
                    return resolve(result);
                }).catch(err => {
                    return reject(err);
                });
            }
        })
    }
}

module.exports = QueryResult
