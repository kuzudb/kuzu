#pragma once

#include "main/connection.h"
#include "main/query_result_fetcher.h"
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
    static bool testStatement(TestStatement* statement, main::Connection& conn,
        std::string& databasePath);
    static bool checkLogicalPlans(std::unique_ptr<main::PreparedStatement>& preparedStatement,
        TestStatement* statement, size_t resultIdx, main::Connection& conn);
    static bool checkLogicalPlan(std::unique_ptr<main::PreparedStatement>& preparedStatement,
        TestStatement* statement, size_t resultIdx, main::Connection& conn, uint32_t planIdx);
    template<main::QueryResultFetcher Fetcher>
    static bool checkResultNumeric(Fetcher& resultTuples, TestStatement* statement,
        size_t resultIdx);

    template<main::QueryResultFetcher Fetcher>
    static std::vector<std::string> convertResultToString(Fetcher& queryResult,
        bool checkOutputOrder = false, bool checkColumnNames = false);
    template<main::QueryResultFetcher Fetcher>
    static std::string convertResultToMD5Hash(Fetcher& queryResult, bool checkOutputOrder,
        bool checkColumnNames); // returns hash and number of values hashed
    template<main::QueryResultFetcher Fetcher>
    static std::string convertResultColumnsToString(Fetcher& queryResult);
    template<main::QueryResultFetcher Fetcher>
    static bool checkPlanResult(Fetcher& result, TestStatement* statement, size_t resultIdx,
        const std::string& planStr, uint32_t planIdx);
};

} // namespace testing
} // namespace kuzu
