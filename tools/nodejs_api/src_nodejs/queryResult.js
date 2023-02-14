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

    hasNext(){
        this.checkForQueryResultClose();
        return this.#queryResult.hasNext();
    }

    getNext(){
        this.checkForQueryResultClose();
        return this.#queryResult.getNext();
    }

    close(){
        if (this.#isClosed){
            return;
        }
        this.#queryResult.close();
        this.#isClosed = true;
    }

    each() {
        if (this.hasNext()) {
            return this.getNext();
        }
        return;
    }

    all() {
        const arr = [];
        try {
            while (this.hasNext()) {
                arr.push(this.getNext());
            }
        }
        catch(error){
            console.log(error);
        }
        return arr;
    }
}

module.exports = QueryResult
