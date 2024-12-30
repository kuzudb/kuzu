#pragma once

#include "main/connection.h"
#include "test_runner/test_group.h"

namespace kuzu {
namespace testing {

class TestRunner {
public:
    static void runTest(TestStatement* statement, main::Connection& conn,
        const std::string& databasePath);

    static std::unique_ptr<planner::LogicalPlan> getLogicalPlan(const std::string& query,
        main::Connection& conn);

private:
    static void testStatement(TestStatement* statement, main::Connection& conn,
        const std::string& databasePath);
    static void checkLogicalPlan(main::Connection& conn, main::QueryResult* queryResult,
        TestStatement* statement, size_t resultIdx);
    static bool checkResultNumeric(main::QueryResult& queryResult, const TestStatement* statement,
        size_t resultIdx);
    static std::vector<std::string> convertResultToString(main::QueryResult& queryResult,
        bool checkOutputOrder = false, bool checkColumnNames = false);
    static std::string convertResultToMD5Hash(main::QueryResult& queryResult, bool checkOutputOrder,
        bool checkColumnNames); // returns hash and number of values hashed
    static std::string convertResultColumnsToString(const main::QueryResult& queryResult);
    static void checkPlanResult(main::Connection& conn, main::QueryResult* result,
        TestStatement* statement, size_t resultIdx);

    static void outputFailedPlan(main::Connection& conn, const TestStatement* statement);
};

} // namespace testing
} // namespace kuzu
