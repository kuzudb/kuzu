require("./common.js");

const importTest = (name, path) => {
  describe(name, () => {
    require(path);
  });
};

describe("kuzu", () => {
  before(() => {
    return initTests();
  });
  importTest("Database", "./test_database.js");
  importTest("Connection", "./test_connection.js");
  importTest("Query result", "./test_query_result.js");
  importTest("Data types", "./test_data_type.js");
  importTest("Query parameters", "./test_parameter.js");
  importTest("Concurrent query execution", "./test_concurrency.js");
  importTest("Version", "./test_version.js");
  importTest("Synchronous API", "./test_sync_api.js");
});
