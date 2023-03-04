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

    async each(rowCallback, doneCallback, errorCallback) {
        this.checkForQueryResultClose();
        let hasError = false;
        while (this.#queryResult.hasNext() && !hasError) {
            await this.getNext().then(row => {
                rowCallback(row);
            }).catch(err => {
                errorCallback(err);
                hasError = true;
            })
        }
        doneCallback();
    }

    async getNext() {
        this.checkForQueryResultClose();
        return new Promise((resolve, reject) => {
            this.#queryResult.getNext((err, result) => {
                if (err) {
                    return reject(err);
                } else {
                    return resolve(result);
                }
            })
        })
    }

    async all(callback = null) {
        this.checkForQueryResultClose();
        if (callback) {
            if (typeof callback !== 'function') {
                throw new Error("If all is provided an argument it must be a callback function");
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
