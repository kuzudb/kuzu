#pragma once

#include "main/connection.h"
#include "test_runner/test_group.h"

namespace kuzu {
namespace testing {

class TestRunner {
public:
    static void runTest(TestStatement* statement, main::Connection& conn,
        std::string& databasePath);

    static std::unique_ptr<planner::LogicalPlan> getLogicalPlan(const std::string& query,
        main::Connection& conn);

private:
    static void testStatement(TestStatement* statement, main::Connection& conn,
        std::string& databasePath);
    static void checkLogicalPlans(std::unique_ptr<main::PreparedStatement>& preparedStatement,
        TestStatement* statement, size_t resultIdx, main::Connection& conn);
    static void checkLogicalPlan(std::unique_ptr<main::PreparedStatement>& preparedStatement,
        TestStatement* statement, size_t resultIdx, main::Connection& conn, uint32_t planIdx);
    static bool checkResultNumeric(main::QueryResult& resultTuples, TestStatement* statement,
        size_t resultIdx);
    static std::vector<std::string> convertResultToString(main::QueryResult& queryResult,
        bool checkOutputOrder = false, bool checkColumnNames = false);
    static std::string convertResultToMD5Hash(main::QueryResult& queryResult, bool checkOutputOrder,
        bool checkColumnNames); // returns hash and number of values hashed
    static std::string convertResultColumnsToString(main::QueryResult& queryResult);
    static void checkPlanResult(std::unique_ptr<main::QueryResult>& result,
        TestStatement* statement, size_t resultIdx, const std::string& planStr, uint32_t planIdx);
};

} // namespace testing
} // namespace kuzu
