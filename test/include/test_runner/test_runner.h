#include "common/file_utils.h"
#include "gtest/gtest.h"
#include "parser/parser.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/planner.h"
#include "test_runner/test_group.h"

namespace kuzu {
namespace testing {

class TestRunner {
public:
    static void runTest(
        const std::vector<std::unique_ptr<TestStatement>>& statements, main::Connection& conn);

    static std::unique_ptr<planner::LogicalPlan> getLogicalPlan(
        const std::string& query, main::Connection& conn);

private:
    static void initializeConnection(TestStatement* statement, main::Connection& conn);
    static bool testStatement(TestStatement* statement, main::Connection& conn);
    static bool checkLogicalPlans(std::unique_ptr<main::PreparedStatement>& preparedStatement,
        TestStatement* statement, main::Connection& conn);
    static bool checkLogicalPlan(std::unique_ptr<main::PreparedStatement>& preparedStatement,
        TestStatement* statement, main::Connection& conn, uint32_t planIdx);
    static std::vector<std::string> convertResultToString(
        main::QueryResult& queryResult, bool checkOutputOrder = false);
    static bool checkPlanResult(std::unique_ptr<main::QueryResult>& result,
        TestStatement* statement, const std::string& planStr, uint32_t planIndex);
};

} // namespace testing
} // namespace kuzu
