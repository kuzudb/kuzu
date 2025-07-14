#include "api_test/api_test.h"
#include "file_descriptor_monitor.h"
#include "gtest/gtest.h"
#include "main/connection.h"
#include "main/database.h"
#include "storage/storage_utils.h"
#include "test_helper/test_helper.h"

using namespace kuzu::main;
using namespace kuzu::testing;

class FileDescriptorLeakTest : public ApiTest {
public:
    void SetUp() override {
        ApiTest::SetUp();
        conn.reset();
        database.reset();
        // Clean up any existing files
        std::error_code ec;
        std::filesystem::remove_all(databasePath, ec);
        std::filesystem::remove(kuzu::storage::StorageUtils::getWALFilePath(databasePath), ec);
        std::filesystem::remove(kuzu::storage::StorageUtils::getShadowFilePath(databasePath), ec);
    }

    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("empty");
    }

    void TearDown() override {
        // Clean up test files
        std::error_code ec;
        std::filesystem::remove_all(databasePath, ec);
        std::filesystem::remove(kuzu::storage::StorageUtils::getWALFilePath(databasePath), ec);
        std::filesystem::remove(kuzu::storage::StorageUtils::getShadowFilePath(databasePath), ec);
    }
};

// Test basic database creation and destruction
TEST_F(FileDescriptorLeakTest, BasicDatabaseLifecycle) {
    ScopedFileDescriptorMonitor monitor("BasicDatabaseLifecycle");
    monitor.trackDatabaseFiles(databasePath);

    {
        Database database(databasePath);
        Connection conn(&database);

        // Create a simple table
        auto result =
            conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());

        // Insert some data
        result = conn.query("CREATE (:Person {name: 'Alice', age: 30})");
        ASSERT_TRUE(result->isSuccess());

        result = conn.query("CREATE (:Person {name: 'Bob', age: 25})");
        ASSERT_TRUE(result->isSuccess());

        // Query the data
        result = conn.query("MATCH (p:Person) RETURN p.name, p.age");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), 2);
    }

    // Database should be properly closed here
    // Monitor will automatically check for leaks in destructor
}

// Test multiple database connections
TEST_F(FileDescriptorLeakTest, MultipleDatabaseConnections) {
    ScopedFileDescriptorMonitor monitor("MultipleDatabaseConnections");
    monitor.trackDatabaseFiles(databasePath);

    {
        Database database(databasePath);

        // Create multiple connections
        std::vector<std::unique_ptr<Connection>> connections;
        for (int i = 0; i < 5; ++i) {
            connections.push_back(std::make_unique<Connection>(&database));
        }

        // Use all connections
        for (size_t i = 0; i < connections.size(); ++i) {
            auto result = connections[i]->query("CREATE NODE TABLE Person" + std::to_string(i) +
                                                "(name STRING, age INT64, PRIMARY KEY (name))");
            ASSERT_TRUE(result->isSuccess());
        }

        // Close connections explicitly
        connections.clear();
    }

    // All connections and database should be properly closed
}

// Test read-only database access
TEST_F(FileDescriptorLeakTest, ReadOnlyDatabaseAccess) {
    ScopedFileDescriptorMonitor monitor("ReadOnlyDatabaseAccess");
    monitor.trackDatabaseFiles(databasePath);

    // First create a database with some data
    {
        Database database(databasePath);
        Connection conn(&database);

        auto result =
            conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());

        result = conn.query("CREATE (:Person {name: 'Alice', age: 30})");
        ASSERT_TRUE(result->isSuccess());
    }

    // Now open in read-only mode
    {
        SystemConfig config;
        config.readOnly = true;
        Database database(databasePath, config);
        Connection conn(&database);

        auto result = conn.query("MATCH (p:Person) RETURN p.name, p.age");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), 1);
    }
}

// Test database with transactions
TEST_F(FileDescriptorLeakTest, DatabaseTransactions) {
    ScopedFileDescriptorMonitor monitor("DatabaseTransactions");
    monitor.trackDatabaseFiles(databasePath);

    {
        Database database(databasePath);
        Connection conn(&database);

        // Create table
        auto result =
            conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());

        // Test successful transaction
        conn.query("BEGIN TRANSACTION;");
        result = conn.query("CREATE (:Person {name: 'Alice', age: 30})");
        ASSERT_TRUE(result->isSuccess());
        conn.query("COMMIT;");

        // Test rollback transaction
        conn.query("BEGIN TRANSACTION;");
        result = conn.query("CREATE (:Person {name: 'Bob', age: 25})");
        ASSERT_TRUE(result->isSuccess());
        conn.query("ROLLBACK;");

        // Verify only Alice exists
        result = conn.query("MATCH (p:Person) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 1);
    }
}

// Test database with large data operations
TEST_F(FileDescriptorLeakTest, LargeDataOperations) {
    ScopedFileDescriptorMonitor monitor("LargeDataOperations");
    monitor.trackDatabaseFiles(databasePath);

    {
        Database database(databasePath);
        Connection conn(&database);

        // Create table
        auto result =
            conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());

        // Insert large amount of data to trigger spilling
        conn.query("BEGIN TRANSACTION;");
        for (int i = 0; i < 1000; ++i) {
            result = conn.query("CREATE (:Person {name: 'Person" + std::to_string(i) +
                                "', age: " + std::to_string(i % 100) + "})");
            ASSERT_TRUE(result->isSuccess());
        }
        conn.query("COMMIT;");

        // Perform large query that might require spilling
        result =
            conn.query("MATCH (p:Person) WHERE p.age > 50 RETURN p.name, p.age ORDER BY p.age");
        ASSERT_TRUE(result->isSuccess());

        // Count results
        size_t count = 0;
        while (result->hasNext()) {
            result->getNext();
            count++;
        }
        ASSERT_GT(count, 0);
    }
}

// Test database exceptions and error handling
TEST_F(FileDescriptorLeakTest, ExceptionHandling) {
    ScopedFileDescriptorMonitor monitor("ExceptionHandling");
    monitor.trackDatabaseFiles(databasePath);

    {
        Database database(databasePath);
        Connection conn(&database);

        // Create table
        auto result =
            conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());

        // Insert valid data
        result = conn.query("CREATE (:Person {name: 'Alice', age: 30})");
        ASSERT_TRUE(result->isSuccess());

        // Try to insert duplicate primary key (should fail)
        result = conn.query("CREATE (:Person {name: 'Alice', age: 25})");
        ASSERT_FALSE(result->isSuccess());

        // Try invalid query syntax
        result = conn.query("INVALID QUERY SYNTAX");
        ASSERT_FALSE(result->isSuccess());

        // Try to query non-existent table
        result = conn.query("MATCH (p:NonExistent) RETURN p.name");
        ASSERT_FALSE(result->isSuccess());

        // Verify database is still functional
        result = conn.query("MATCH (p:Person) RETURN p.name");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), 1);
    }
}

// Test database with forced checkpoint
TEST_F(FileDescriptorLeakTest, ForcedCheckpoint) {
    ScopedFileDescriptorMonitor monitor("ForcedCheckpoint");
    monitor.trackDatabaseFiles(databasePath);

    {
        SystemConfig config;
        config.forceCheckpointOnClose = true;
        Database database(databasePath, config);
        Connection conn(&database);

        // Create table and data
        auto result =
            conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());

        result = conn.query("CREATE (:Person {name: 'Alice', age: 30})");
        ASSERT_TRUE(result->isSuccess());

        // Perform some operations that would create WAL entries
        for (int i = 0; i < 10; ++i) {
            result = conn.query("CREATE (:Person {name: 'Person" + std::to_string(i) +
                                "', age: " + std::to_string(i) + "})");
            ASSERT_TRUE(result->isSuccess());
        }
    }

    // Database should force checkpoint on close
}
