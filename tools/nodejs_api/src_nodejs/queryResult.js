class QueryResult {
    #queryResult
    #isClosed
    constructor(queryResult) {
        if (typeof queryResult !== "object" || queryResult.constructor.name !==  "NodeQueryResult"){
            throw new Error("QueryResult constructor requires a NodeQueryResult object as an argument");
        }
        this.#queryResult = queryResult;
        this.#isClosed = false;
    }

    checkForQueryResultClose(){
        if (this.#isClosed){
            throw new Error("Query Result is Closed");
        }
    }

    close(){
        this.checkForQueryResultClose();
        this.#queryResult.close();
        this.#isClosed = true;
    }

     async each(rowCallback, doneCallback) {
        this.checkForQueryResultClose();
         if (typeof rowCallback !== 'function' || rowCallback.length !== 2 || typeof doneCallback !== 'function' || doneCallback.length !== 0) {
             throw new Error("all must have 2 callbacks: rowCallback takes 2 arguments: (err, result), doneCallback takes none");
         }
        await this.#queryResult.each(rowCallback, doneCallback);
    }

    async all(opts = {} ) {
        this.checkForQueryResultClose();
        if ('callback' in opts) {
            const callback = opts['callback'];
            if (typeof callback !== 'function') {
                throw new Error("if execute is given a callback, it must take 2 arguments: (err, result)");
            }
            await this.#queryResult.all(callback);
        } else {
            return new Promise(async (resolve, reject) => {
                await this.#queryResult.all((err, result) => {
                    if (err) {
                        return reject(err);
                    }
                    return resolve(result);
                });
            })
        }
    }
}

module.exports = QueryResult
