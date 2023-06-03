class QueryResult {
  constructor(connection, queryResult) {
    assert(
      typeof connection === "object" &&
        connection.constructor.name === "Connection"
    );
    assert(
      typeof queryResult === "object" &&
        queryResult.constructor.name === "NodeQueryResult"
    );
    this._connection = connection;
    this._queryResult = queryResult;
  }

  resetIterator() {
    this._queryResult.resetIterator();
  }

  hasNext() {
    return this._queryResult.hasNext();
  }

  getNext() {
    return new Promise((resolve, reject) => {
      this._queryResult.getNextAsync((err, result) => {
        if (err) {
          return reject(err);
        }
        return resolve(result);
      });
    });
  }

  each(resultCallback, doneCallback, errorCallback) {
    if (!this.hasNext()) {
      return doneCallback();
    }
    this.getNext()
      .then((row) => {
        resultCallback(row);
        this.each(resultCallback, doneCallback, errorCallback);
      })
      .catch((err) => {
        errorCallback(err);
      });
  }

  async getAll() {
    this._queryResult.resetIterator();
    const result = [];
    while (this.hasNext()) {
      result.push(await this.getNext());
    }
    return result;
  }

  all(resultCallback, errorCallback) {
    this.getAll()
      .then((result) => {
        resultCallback(result);
      })
      .catch((err) => {
        errorCallback(err);
      });
  }

  getColumnDataTypes() {
    return new Promise((resolve, reject) => {
      this._queryResult.getColumnDataTypesAsync((err, result) => {
        if (err) {
          return reject(err);
        }
        return resolve(result);
      });
    });
  }

  getColumnNames() {
    return new Promise((resolve, reject) => {
      this._queryResult.getColumnNamesAsync((err, result) => {
        if (err) {
          return reject(err);
        }
        return resolve(result);
      });
    });
  }
}

module.exports = QueryResult;
