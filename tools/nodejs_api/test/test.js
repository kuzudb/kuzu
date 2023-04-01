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
  importTest("datatype", "./testDatatype.js");
  importTest("exception", "./testException.js");
  importTest("getHeader", "./testGetHeader.js");
  importTest("parameter", "./testParameter.js");
});
