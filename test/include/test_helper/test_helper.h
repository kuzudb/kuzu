#pragma once

#include <cstring>

#include "common/file_utils.h"
#include "main/kuzu.h"
#include "parser/parser.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/planner.h"

using namespace std;
using namespace kuzu::main;

namespace kuzu {
namespace testing {

struct TestQueryConfig {
    string name;
    string query;
    uint64_t numThreads = 4;
    string encodedJoin;
    uint64_t expectedNumTuples = 0;
    vector<string> expectedTuples;
    bool enumerate = false;
    bool checkOutputOrder = false;
};

class TestHelper {
public:
    static vector<unique_ptr<TestQueryConfig>> parseTestFile(
        const string& path, bool checkOutputOrder = false);

    static bool testQueries(vector<unique_ptr<TestQueryConfig>>& configs, Connection& conn);

    static vector<string> convertResultToString(
        QueryResult& queryResult, bool checkOutputOrder = false);

    static void executeCypherScript(const string& path, Connection& conn);

    static constexpr char SCHEMA_FILE_NAME[] = "schema.cypher";
    static constexpr char COPY_CSV_FILE_NAME[] = "copy.cypher";

    static string getTmpTestDir() { return appendKuzuRootPath("test/unittest_temp/"); }

    static string appendKuzuRootPath(const string& path) {
        return KUZU_ROOT_DIRECTORY + string("/") + path;
    }

private:
    static void initializeConnection(TestQueryConfig* config, Connection& conn);
    static bool testQuery(TestQueryConfig* config, Connection& conn);
};

} // namespace testing
} // namespace kuzu
