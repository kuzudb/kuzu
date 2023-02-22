const callbackWrapper = require("./common.js");

class QueryResult {
    #queryResult
    #isClosed
    constructor(queryResult) {
        this.#queryResult = queryResult;
        this.#isClosed = false;
    }

    checkForQueryResultClose(){
        if (this.#isClosed){
            throw new Error("Query Result is Closed");
        }
    }

    close(){
        if (this.#isClosed){
            return;
        }
        this.#queryResult.close();
        this.#isClosed = true;
    }

    each(eachCallback= null, doneCallback = null) {
        this.checkForQueryResultClose();
        this.#queryResult.each((err, result) => {
            callbackWrapper(err, () => {
                if (eachCallback) { eachCallback(result); }
                // if (doneCallback && result.at(-1) == 0) {
                //     doneCallback();
                // }
            });
        }, (err) => {
            if (doneCallback) {
                if (err) { console.log(err); }
                else doneCallback();
            }
        });
    }

    all(callback = null) {
        this.checkForQueryResultClose();
        if (callback) {
            this.#queryResult.all((err, result) => {
                callbackWrapper(err, () => {
                    callback(result);
                });
            });
        } else {
            return new Promise ((resolve, reject) => {
                this.#queryResult.all((err, result) => {
                    if (err) { return reject(err); }
                    return resolve(result);
                });
            })
        }
    }
}

module.exports = QueryResult
