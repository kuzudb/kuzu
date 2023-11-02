#pragma once

#include "main/kuzu.h"

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
    static constexpr char E2E_TEST_FILES_DIRECTORY[] = "test/test_files";
    static constexpr char SCHEMA_FILE_NAME[] = "schema.cypher";
    static constexpr char COPY_FILE_NAME[] = "copy.cypher";
    static constexpr char PARQUET_TEMP_DATASET_PATH[] = "dataset/parquet_temp_";
    static constexpr char TMP_TEST_DIR[] = "test/unittest_temp_";
    static constexpr char TEST_ANSWERS_PATH[] = "test/answers";
    static constexpr char TEST_STATEMENTS_PATH[] = "test/statements";
    static constexpr char DEFAULT_CONN_NAME[] = "conn_default";

    static std::vector<std::unique_ptr<TestQueryConfig>> parseTestFile(
        const std::string& path, bool checkOutputOrder = false);

    static std::vector<std::string> convertResultToString(
        main::QueryResult& queryResult, bool checkOutputOrder = false);

    static void executeScript(const std::string& path, main::Connection& conn);

    static std::string getTestListFile() {
        return appendKuzuRootPath(std::string(E2E_TEST_FILES_DIRECTORY) + "/test_list");
    }

    static std::string appendKuzuRootPath(const std::string& path) {
        return KUZU_ROOT_DIRECTORY + std::string("/") + path;
    }

    static std::string getMillisecondsSuffix();

private:
    static void initializeConnection(TestQueryConfig* config, main::Connection& conn);
};

} // namespace testing
} // namespace kuzu
