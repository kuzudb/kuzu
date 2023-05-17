#include "main/kuzu.h"
#include "test_helper/test_parser.h"

using namespace kuzu::main;

namespace kuzu {
namespace testing {

struct TestStatement {
    std::string name;
    std::string query;
    std::string blockName;
    uint64_t numThreads = 4;
    std::string encodedJoin;
    uint64_t expectedNumTuples = 0;
    bool expectedError = false;
    bool expectedOk = false;
    std::vector<std::string> expectedTuples;
    std::string errorMessage;
    bool enumerate = false;
    bool checkOutputOrder = false;
    bool isVariableStatement = false;
};

class TestCase {
public:
    std::string group;
    std::string name;
    std::string dataset;
    std::vector<std::unique_ptr<TestStatement>> statements;
    std::vector<std::unique_ptr<TestStatement>> variableStatements;

    bool skipTest = false;

    bool isHeaderValid() const { return !group.empty() && !name.empty() && !dataset.empty(); }

    bool isValid() const { return isHeaderValid() && !statements.empty(); }

    void parseTestFile(const std::string& path);

private:
    void parseHeader(TestParser& parser);
    void parseBody(TestParser& parser);
    void extractExpectedResult(TestParser& parser, TestStatement* currentStatement);
    void extractStatementBlock(TestParser& parser);
    TestStatement* extractStatement(TestParser& parser, TestStatement* currentStatement);
    TestStatement* addNewStatement(std::vector<std::unique_ptr<TestStatement>>& statements);
    std::string paramsToString(const std::vector<std::string>& params);
    void checkMinimumParams(TestParser& parser, int minimumParams);
};

} // namespace testing
} // namespace kuzu
