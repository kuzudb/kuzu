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

private:
    static void initializeConnection(TestQueryConfig* config, Connection& conn);
    static bool testQuery(TestQueryConfig* config, Connection& conn);
};

// Loads a database from raw csv files and creates a disk-based DatabaseConfig. To have an in-memory
// version, the databaseConfig field needs to be manually changed as in InMemoryDBLoadedTest.
class BaseGraphTest : public Test {
public:
    void SetUp() override {
        systemConfig = make_unique<SystemConfig>();
        databaseConfig = make_unique<DatabaseConfig>(TestHelper::TEMP_TEST_DIR);
    }

    void TearDown() override { FileUtils::removeDir(TestHelper::TEMP_TEST_DIR); }

    // The default behaviour of BaseGraphTest is to load tinySnb. Whoever inherit this class should
    // implement their own initGraph().
    virtual void initGraph();

    inline void createDBAndConn(
        TransactionTestType transactionTestType = TransactionTestType::NORMAL_EXECUTION) {
        database = make_unique<Database>(*databaseConfig, *systemConfig);
        conn = make_unique<Connection>(database.get());
        if (transactionTestType == TransactionTestType::NORMAL_EXECUTION) {
            initGraph();
        }
    }

    void commitOrRollbackConnection(bool isCommit, TransactionTestType transactionTestType);

public:
    unique_ptr<SystemConfig> systemConfig;
    unique_ptr<DatabaseConfig> databaseConfig;
    unique_ptr<Database> database;
    unique_ptr<Connection> conn;
    string createNodeCmdPrefix = "CREATE NODE TABLE ";
    string createRelCmdPrefix = "CREATE REL ";
};

class DBTest : public BaseGraphTest {

public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        systemConfig->largePageBufferPoolSize = (1ull << 23);
        createDBAndConn();
    }
};

class InMemoryDBTest : public BaseGraphTest {

public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        systemConfig->defaultPageBufferPoolSize = (1ul << 23);
        databaseConfig->inMemoryMode = true;
        createDBAndConn();
    }
};

} // namespace testing
} // namespace graphflow
