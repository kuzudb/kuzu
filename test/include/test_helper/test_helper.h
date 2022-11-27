#pragma once

#include <cstring>

#include "common/file_utils.h"
#include "gtest/gtest.h"
#include "main/kuzu.h"

using namespace std;
using namespace kuzu::main;
using ::testing::Test;

namespace kuzu {
namespace testing {

enum class TransactionTestType : uint8_t {
    NORMAL_EXECUTION = 0,
    RECOVERY = 1,
};

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
    static constexpr char COPY_CSV_FILE_NAME[] = "copy_csv.cypher";

    static string getTmpTestDir() { return appendKuzuRootPath("test/unittest_temp/"); }

    static string appendKuzuRootPath(const string& path) {
        return KUZU_ROOT_DIRECTORY + string("/") + path;
    }

private:
    static void initializeConnection(TestQueryConfig* config, Connection& conn);
    static bool testQuery(TestQueryConfig* config, Connection& conn);
};

class BaseGraphTest : public Test {
public:
    void SetUp() override {
        systemConfig =
            make_unique<SystemConfig>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        databaseConfig = make_unique<DatabaseConfig>(TestHelper::getTmpTestDir());
    }

    virtual string getInputCSVDir() = 0;

    void TearDown() override { FileUtils::removeDir(TestHelper::getTmpTestDir()); }

    inline void createDBAndConn() {
        database = make_unique<Database>(*databaseConfig, *systemConfig);
        conn = make_unique<Connection>(database.get());
        spdlog::set_level(spdlog::level::info);
    }

    void initGraph();

    void commitOrRollbackConnection(bool isCommit, TransactionTestType transactionTestType) const;

protected:
    // Static functions to access Database's non-public properties/interfaces.
    static inline catalog::Catalog* getCatalog(Database& database) {
        return database.catalog.get();
    }
    static inline storage::StorageManager* getStorageManager(Database& database) {
        return database.storageManager.get();
    }
    static inline storage::BufferManager* getBufferManager(Database& database) {
        return database.bufferManager.get();
    }
    static inline storage::MemoryManager* getMemoryManager(Database& database) {
        return database.memoryManager.get();
    }
    static inline transaction::TransactionManager* getTransactionManager(Database& database) {
        return database.transactionManager.get();
    }
    static inline uint64_t getDefaultBMSize(Database& database) {
        return database.systemConfig.defaultPageBufferPoolSize;
    }
    static inline uint64_t getLargeBMSize(Database& database) {
        return database.systemConfig.largePageBufferPoolSize;
    }
    static inline WAL* getWAL(Database& database) { return database.wal.get(); }
    static inline void commitAndCheckpointOrRollback(Database& database,
        transaction::Transaction* writeTransaction, bool isCommit,
        bool skipCheckpointForTestingRecovery = false) {
        database.commitAndCheckpointOrRollback(
            writeTransaction, isCommit, skipCheckpointForTestingRecovery);
    }
    static inline QueryProcessor* getQueryProcessor(Database& database) {
        return database.queryProcessor.get();
    }

    // Static functions to access Connection's non-public properties/interfaces.
    static inline Connection::ConnectionTransactionMode getTransactionMode(Connection& connection) {
        return connection.getTransactionMode();
    }
    static inline void setTransactionModeNoLock(
        Connection& connection, Connection::ConnectionTransactionMode newTransactionMode) {
        connection.setTransactionModeNoLock(newTransactionMode);
    }
    static inline void commitButSkipCheckpointingForTestingRecovery(Connection& connection) {
        connection.commitButSkipCheckpointingForTestingRecovery();
    }
    static inline void rollbackButSkipCheckpointingForTestingRecovery(Connection& connection) {
        connection.rollbackButSkipCheckpointingForTestingRecovery();
    }
    static inline Transaction* getActiveTransaction(Connection& connection) {
        return connection.getActiveTransaction();
    }
    static inline uint64_t getMaxNumThreadForExec(Connection& connection) {
        return connection.getMaxNumThreadForExec();
    }
    static inline uint64_t getActiveTransactionID(Connection& connection) {
        return connection.getActiveTransactionID();
    }
    static inline bool hasActiveTransaction(Connection& connection) {
        return connection.hasActiveTransaction();
    }
    static inline void commitNoLock(Connection& connection) { connection.commitNoLock(); }
    static inline void rollbackIfNecessaryNoLock(Connection& connection) {
        connection.rollbackIfNecessaryNoLock();
    }

    void validateColumnFilesExistence(string fileName, bool existence, bool hasOverflow);

    void validateListFilesExistence(
        string fileName, bool existence, bool hasOverflow, bool hasHeader);

    void validateNodeColumnFilesExistence(
        NodeTableSchema* nodeTableSchema, DBFileType dbFileType, bool existence);

    void validateRelColumnAndListFilesExistence(
        RelTableSchema* relTableSchema, DBFileType dbFileType, bool existence);

private:
    static inline bool containsOverflowFile(DataTypeID typeID) {
        return typeID == STRING || typeID == LIST;
    }

    void validateRelPropertyFiles(catalog::RelTableSchema* relTableSchema, table_id_t tableID,
        RelDirection relDirection, bool isColumnProperty, DBFileType dbFileType, bool existence);

public:
    unique_ptr<SystemConfig> systemConfig;
    unique_ptr<DatabaseConfig> databaseConfig;
    unique_ptr<Database> database;
    unique_ptr<Connection> conn;
};

// This class starts database without initializing graph.
class EmptyDBTest : public BaseGraphTest {
    string getInputCSVDir() override { throw NotImplementedException("getInputCSVDir()"); }
};

// This class starts database in on-disk mode.
class DBTest : public BaseGraphTest {

public:
    void SetUp() override {
        BaseGraphTest::SetUp();
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
} // namespace kuzu
