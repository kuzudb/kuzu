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

    each() {
        return;
    }

    all(callback) {
        this.checkForQueryResultClose();
        this.#queryResult.all((err, result) => {
            if (err){
                console.log("There is an error!!!" + err);
                throw err;
            } else {
                callback(result);
            }
        });
    }
}

module.exports = QueryResult
