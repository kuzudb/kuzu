#pragma once

#include <cstring>

#include "common/file_utils.h"
#include "main/kuzu.h"
#include "parser/parser.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/planner.h"

using namespace kuzu::main;

namespace kuzu {
namespace testing {

struct TestQueryConfig {
    std::string name;
    std::string query;
    uint64_t numThreads = 4;
    std::string encodedJoin;
    uint64_t expectedNumTuples = 0;
    std::vector<std::string> expectedTuples;
    bool enumerate = false;
    bool checkOutputOrder = false;
};

class TestHelper {
public:
    static std::vector<std::unique_ptr<TestQueryConfig>> parseTestFile(
        const std::string& path, bool checkOutputOrder = false);

    static bool testQueries(
        std::vector<std::unique_ptr<TestQueryConfig>>& configs, Connection& conn);

    static std::vector<std::string> convertResultToString(
        QueryResult& queryResult, bool checkOutputOrder = false);

    static void executeScript(const std::string& path, Connection& conn);

    static constexpr char SCHEMA_FILE_NAME[] = "schema.cypher";
    static constexpr char COPY_FILE_NAME[] = "copy.cypher";

    static std::string getTmpTestDir() { return appendKuzuRootPath("test/unittest_temp/"); }

    static std::string appendKuzuRootPath(const std::string& path) {
        return KUZU_ROOT_DIRECTORY + std::string("/") + path;
    }

    static std::unique_ptr<planner::LogicalPlan> getLogicalPlan(
        const std::string& query, Connection& conn);

private:
    static void initializeConnection(TestQueryConfig* config, Connection& conn);
    static bool testQuery(TestQueryConfig* config, Connection& conn);
};

} // namespace testing
} // namespace kuzu
