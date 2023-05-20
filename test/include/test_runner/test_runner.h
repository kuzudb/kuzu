#include "common/file_utils.h"
#include "gtest/gtest.h"
#include "parser/parser.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/planner.h"
#include "test_runner/test_case.h"

using namespace kuzu::planner;

namespace kuzu {
namespace testing {

class TestRunner {
public:
    static void runTest(
        const std::vector<std::unique_ptr<TestStatement>>& statements, Connection& conn);

    static std::unique_ptr<planner::LogicalPlan> getLogicalPlan(
        const std::string& query, Connection& conn);

private:
    static void initializeConnection(TestStatement* statement, Connection& conn);
    static bool testStatement(TestStatement* statement, Connection& conn);
    static bool checkLogicalPlans(std::unique_ptr<PreparedStatement>& preparedStatement,
        TestStatement* statement, Connection& conn);
    static std::vector<std::string> convertResultToString(
        QueryResult& queryResult, bool checkOutputOrder = false);
};

} // namespace testing
} // namespace kuzu
