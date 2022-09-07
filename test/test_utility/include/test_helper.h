#pragma once

#include "gtest/gtest.h"

#include "src/common/include/file_utils.h"
#include "src/main/include/graphflowdb.h"

using namespace std;
using namespace graphflow::main;
using ::testing::Test;

namespace graphflow {
namespace testing {

enum class TransactionTestType : uint8_t {
    NORMAL_EXECUTION = 0,
    RECOVERY = 1,
};

struct TestQueryConfig {
    string name;
    string query;
    uint64_t numThreads = 1;
    string encodedJoin;
    uint64_t expectedNumTuples = 0;
    vector<string> expectedTuples;
    bool enumerate = false;
    bool checkOutputOrder = false;
};

class TestHelper {
public:
    static constexpr char TEMP_TEST_DIR[] = "test/unittest_temp/";

    static vector<unique_ptr<TestQueryConfig>> parseTestFile(
        const string& path, bool checkOutputOrder = false);

    static bool testQueries(vector<unique_ptr<TestQueryConfig>>& configs, Connection& conn);

    static vector<string> convertResultToString(
        QueryResult& queryResult, bool checkOutputOrder = false);

    static void executeCypherScript(const string& path, Connection& conn);

    static constexpr char SCHEMA_FILE_NAME[] = "schema.cypher";
    static constexpr char COPY_CSV_FILE_NAME[] = "copy_csv.cypher";

private:
    static void initializeConnection(TestQueryConfig* config, Connection& conn);
    static bool testQuery(TestQueryConfig* config, Connection& conn);
};

class BaseGraphTest : public Test {
public:
    void SetUp() override {
        systemConfig = make_unique<SystemConfig>();
        databaseConfig = make_unique<DatabaseConfig>(TestHelper::TEMP_TEST_DIR);
    }

    virtual string getInputCSVDir() = 0;

    void TearDown() override { FileUtils::removeDir(TestHelper::TEMP_TEST_DIR); }

    inline void createDBAndConn() {
        database = make_unique<Database>(*databaseConfig, *systemConfig);
        conn = make_unique<Connection>(database.get());
    }

    void initGraph();

    void commitOrRollbackConnection(bool isCommit, TransactionTestType transactionTestType);

public:
    unique_ptr<SystemConfig> systemConfig;
    unique_ptr<DatabaseConfig> databaseConfig;
    unique_ptr<Database> database;
    unique_ptr<Connection> conn;
};

// This class starts database without initializing graph.
class EmptyDBTest : public BaseGraphTest {
    string getInputCSVDir() override { assert(false); }
};

// This class starts database in on-disk mode.
class DBTest : public BaseGraphTest {

public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        systemConfig->largePageBufferPoolSize = (1ull << 23);
        createDBAndConn();
        initGraph();
    }

    inline void runTest(const string& queryFile) {
        auto queryConfigs = TestHelper::parseTestFile(queryFile);
        ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
    }
};

// This class starts database in in-memory mode.
class InMemoryDBTest : public BaseGraphTest {

public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        databaseConfig->inMemoryMode = true;
        createDBAndConn();
        initGraph();
    }
};

} // namespace testing
} // namespace graphflow
