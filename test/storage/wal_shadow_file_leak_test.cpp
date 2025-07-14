#include "file_descriptor_monitor.h"
#include "test_helper/test_helper.h"
#include "gtest/gtest.h"
#include "main/connection.h"
#include "main/database.h"

using namespace kuzu::main;
using namespace kuzu::testing;

class WALShadowFileLeakTest : public ::testing::Test {
public:
    void SetUp() override {
        testDbPath = TestHelper::getTmpTestDir() + "/wal_shadow_test_" + 
                    std::to_string(std::time(nullptr));
        
        // Clean up any existing files
        std::error_code ec;
        std::filesystem::remove_all(testDbPath, ec);
        std::filesystem::remove(testDbPath + ".wal", ec);
        std::filesystem::remove(testDbPath + ".shadow", ec);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(testDbPath, ec);
        std::filesystem::remove(testDbPath + ".wal", ec);
        std::filesystem::remove(testDbPath + ".shadow", ec);
    }

protected:
    std::string testDbPath;
};

// Test WAL file creation and cleanup during normal operations
TEST_F(WALShadowFileLeakTest, WALFileLifecycle) {
    ScopedFileDescriptorMonitor monitor("WALFileLifecycle");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        Database database(testDbPath);
        Connection conn(&database);
        
        // Create table
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        // Start transaction (should create WAL entries)
        conn.beginTransaction();
        
        // Insert data (creates WAL entries)
        result = conn.query("CREATE (:Person {name: 'Alice', age: 30})");
        ASSERT_TRUE(result->isSuccess());
        
        // Check that WAL file might exist
        std::string walPath = testDbPath + ".wal";
        // WAL file may or may not exist at this point, depends on buffer flushing
        
        // Commit transaction
        conn.commit();
        
        // Insert more data in separate transaction
        conn.beginTransaction();
        result = conn.query("CREATE (:Person {name: 'Bob', age: 25})");
        ASSERT_TRUE(result->isSuccess());
        conn.commit();
    }
    
    // WAL file should be cleaned up when database closes
}

// Test WAL file cleanup after rollback
TEST_F(WALShadowFileLeakTest, WALFileRollback) {
    ScopedFileDescriptorMonitor monitor("WALFileRollback");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        Database database(testDbPath);
        Connection conn(&database);
        
        // Create table
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        // Start transaction
        conn.beginTransaction();
        
        // Insert data
        result = conn.query("CREATE (:Person {name: 'Alice', age: 30})");
        ASSERT_TRUE(result->isSuccess());
        
        // Rollback transaction
        conn.rollback();
        
        // Start another transaction
        conn.beginTransaction();
        result = conn.query("CREATE (:Person {name: 'Bob', age: 25})");
        ASSERT_TRUE(result->isSuccess());
        conn.commit();
        
        // Verify only Bob exists
        result = conn.query("MATCH (p:Person) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 1);
    }
}

// Test shadow file creation during checkpoint
TEST_F(WALShadowFileLeakTest, ShadowFileCheckpoint) {
    ScopedFileDescriptorMonitor monitor("ShadowFileCheckpoint");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        DatabaseConfig config;
        config.forceCheckpointOnClose = true;
        Database database(testDbPath, config);
        Connection conn(&database);
        
        // Create table
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        // Insert substantial data to trigger checkpoint
        conn.beginTransaction();
        for (int i = 0; i < 50; ++i) {
            result = conn.query("CREATE (:Person {name: 'Person" + std::to_string(i) + 
                               "', age: " + std::to_string(i) + "})");
            ASSERT_TRUE(result->isSuccess());
        }
        conn.commit();
        
        // Shadow file may be created during checkpoint operations
        std::string shadowPath = testDbPath + ".shadow";
        // Shadow file existence depends on timing and checkpoint behavior
    }
    
    // Shadow file should be cleaned up after checkpoint completes
}

// Test WAL file behavior with multiple transactions
TEST_F(WALShadowFileLeakTest, MultipleTransactionsWAL) {
    ScopedFileDescriptorMonitor monitor("MultipleTransactionsWAL");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        Database database(testDbPath);
        Connection conn(&database);
        
        // Create table
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        // Create multiple concurrent-like transactions
        for (int i = 0; i < 10; ++i) {
            conn.beginTransaction();
            result = conn.query("CREATE (:Person {name: 'Person" + std::to_string(i) + 
                               "', age: " + std::to_string(i) + "})");
            ASSERT_TRUE(result->isSuccess());
            
            if (i % 3 == 0) {
                conn.rollback(); // Rollback every 3rd transaction
            } else {
                conn.commit();
            }
        }
        
        // Verify correct number of records
        result = conn.query("MATCH (p:Person) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        int64_t count = result->getNext()->getValue(0)->getValue<int64_t>();
        ASSERT_EQ(count, 7); // 10 - 3 rolled back transactions
    }
}

// Test WAL file behavior with transaction exceptions
TEST_F(WALShadowFileLeakTest, TransactionExceptions) {
    ScopedFileDescriptorMonitor monitor("TransactionExceptions");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        Database database(testDbPath);
        Connection conn(&database);
        
        // Create table
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        // Insert valid data
        conn.beginTransaction();
        result = conn.query("CREATE (:Person {name: 'Alice', age: 30})");
        ASSERT_TRUE(result->isSuccess());
        conn.commit();
        
        // Start transaction and try to insert duplicate primary key
        conn.beginTransaction();
        result = conn.query("CREATE (:Person {name: 'Alice', age: 25})");
        ASSERT_FALSE(result->isSuccess()); // Should fail due to duplicate key
        
        // Transaction should be automatically rolled back
        // Try to insert different data
        result = conn.query("CREATE (:Person {name: 'Bob', age: 25})");
        ASSERT_TRUE(result->isSuccess());
        conn.commit();
        
        // Verify data
        result = conn.query("MATCH (p:Person) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 2);
    }
}

// Test shadow file behavior with database recovery
TEST_F(WALShadowFileLeakTest, DatabaseRecovery) {
    ScopedFileDescriptorMonitor monitor("DatabaseRecovery");
    monitor.trackDatabaseFiles(testDbPath);
    
    // Create database and insert data
    {
        Database database(testDbPath);
        Connection conn(&database);
        
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        result = conn.query("CREATE (:Person {name: 'Alice', age: 30})");
        ASSERT_TRUE(result->isSuccess());
        
        // Don't force checkpoint, let WAL file persist
    }
    
    // Reopen database (should trigger recovery)
    {
        Database database(testDbPath);
        Connection conn(&database);
        
        // Verify data was recovered
        auto result = conn.query("MATCH (p:Person) RETURN p.name, p.age");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), 1);
        
        auto tuple = result->getNext();
        ASSERT_EQ(tuple->getValue(0)->toString(), "Alice");
        ASSERT_EQ(tuple->getValue(1)->getValue<int64_t>(), 30);
    }
}

// Test WAL file behavior with large transactions
TEST_F(WALShadowFileLeakTest, LargeTransactionWAL) {
    ScopedFileDescriptorMonitor monitor("LargeTransactionWAL");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        Database database(testDbPath);
        Connection conn(&database);
        
        // Create table
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        // Create large transaction
        conn.beginTransaction();
        for (int i = 0; i < 500; ++i) {
            result = conn.query("CREATE (:Person {name: 'Person" + std::to_string(i) + 
                               "', age: " + std::to_string(i % 100) + "})");
            ASSERT_TRUE(result->isSuccess());
        }
        conn.commit();
        
        // Verify all data was committed
        result = conn.query("MATCH (p:Person) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 500);
    }
}

// Test shadow file cleanup after failed checkpoint
TEST_F(WALShadowFileLeakTest, FailedCheckpointCleanup) {
    ScopedFileDescriptorMonitor monitor("FailedCheckpointCleanup");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        Database database(testDbPath);
        Connection conn(&database);
        
        // Create table
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        // Insert data
        conn.beginTransaction();
        for (int i = 0; i < 100; ++i) {
            result = conn.query("CREATE (:Person {name: 'Person" + std::to_string(i) + 
                               "', age: " + std::to_string(i) + "})");
            ASSERT_TRUE(result->isSuccess());
        }
        conn.commit();
        
        // Normal database close should clean up properly
        // Even if checkpoint encounters issues, files should be cleaned up
    }
}

// Test concurrent access to WAL file
TEST_F(WALShadowFileLeakTest, ConcurrentWALAccess) {
    ScopedFileDescriptorMonitor monitor("ConcurrentWALAccess");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        Database database(testDbPath);
        
        // Create multiple connections
        std::vector<std::unique_ptr<Connection>> connections;
        for (int i = 0; i < 3; ++i) {
            connections.push_back(std::make_unique<Connection>(&database));
        }
        
        // Initialize table
        auto result = connections[0]->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        // Use connections concurrently
        for (size_t i = 0; i < connections.size(); ++i) {
            connections[i]->beginTransaction();
            result = connections[i]->query("CREATE (:Person {name: 'Person" + std::to_string(i) + 
                                           "', age: " + std::to_string(i * 10) + "})");
            ASSERT_TRUE(result->isSuccess());
            connections[i]->commit();
        }
        
        // Verify all data
        result = connections[0]->query("MATCH (p:Person) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 3);
        
        // Clean up connections
        connections.clear();
    }
}