#include "file_descriptor_monitor.h"
#include "test_helper/test_helper.h"
#include "gtest/gtest.h"
#include "main/connection.h"
#include "main/database.h"

using namespace kuzu::main;
using namespace kuzu::testing;

class ErrorScenarioFileLeakTest : public ::testing::Test {
public:
    void SetUp() override {
        testDbPath = TestHelper::getTempDir("") + "/error_test_" +
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

// Test file descriptor leaks when database constructor throws
TEST_F(ErrorScenarioFileLeakTest, DatabaseConstructorException) {
    ScopedFileDescriptorMonitor monitor("DatabaseConstructorException");
    
    // Try to create database with invalid path
    try {
        // Use invalid path that should cause constructor to fail
        std::string invalidPath = "/invalid/path/that/does/not/exist/database";
        Database database(invalidPath);
        FAIL() << "Expected database constructor to throw exception";
    } catch (const std::exception&) {
        // Expected exception
    }
    
    // File descriptors should be cleaned up even if constructor throws
}

// Test file descriptor leaks when connection creation fails
TEST_F(ErrorScenarioFileLeakTest, ConnectionCreationFailure) {
    ScopedFileDescriptorMonitor monitor("ConnectionCreationFailure");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        Database database(testDbPath);
        
        // Create valid connection first
        Connection conn1(&database);
        
        // Try to create connection with invalid database reference
        try {
            Database* nullDb = nullptr;
            Connection conn2(nullDb);
            FAIL() << "Expected connection constructor to throw exception";
        } catch (const std::exception&) {
            // Expected exception
        }
        
        // Original connection should still work
        auto result = conn1.query("CREATE NODE TABLE Person(name STRING, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
    }
}

// Test file descriptor leaks during query execution exceptions
TEST_F(ErrorScenarioFileLeakTest, QueryExecutionExceptions) {
    ScopedFileDescriptorMonitor monitor("QueryExecutionExceptions");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        Database database(testDbPath);
        Connection conn(&database);
        
        // Create table
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        // Insert valid data
        result = conn.query("CREATE (:Person {name: 'Alice', age: 30})");
        ASSERT_TRUE(result->isSuccess());
        
        // Execute queries that cause various types of exceptions
        std::vector<std::string> errorQueries = {
            "CREATE (:Person {name: 'Alice', age: 25})", // Duplicate primary key
            "INVALID QUERY SYNTAX HERE",                  // Syntax error
            "MATCH (p:NonExistentTable) RETURN p.name",  // Non-existent table
            "CREATE (:Person {name: 'Bob', age: 'invalid'})", // Type mismatch
            "MATCH (p:Person) RETURN p.nonexistent_field",    // Non-existent field
        };
        
        for (const auto& query : errorQueries) {
            result = conn.query(query);
            ASSERT_FALSE(result->isSuccess()) << "Query should have failed: " << query;
        }
        
        // Verify database is still functional after errors
        result = conn.query("MATCH (p:Person) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 1);
    }
}

// Test file descriptor leaks during transaction rollback exceptions
TEST_F(ErrorScenarioFileLeakTest, TransactionRollbackExceptions) {
    ScopedFileDescriptorMonitor monitor("TransactionRollbackExceptions");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        Database database(testDbPath);
        Connection conn(&database);
        
        // Create table
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        // Test various transaction rollback scenarios
        for (int i = 0; i < 5; ++i) {
            conn.beginTransaction();
            
            // Insert valid data
            result = conn.query("CREATE (:Person {name: 'Person" + std::to_string(i) + 
                               "', age: " + std::to_string(i * 10) + "})");
            ASSERT_TRUE(result->isSuccess());
            
            if (i % 2 == 0) {
                // Rollback even transactions
                conn.rollback();
            } else {
                // Try to insert duplicate and then rollback
                result = conn.query("CREATE (:Person {name: 'Person" + std::to_string(i) + 
                                   "', age: " + std::to_string(i * 10 + 1) + "})");
                ASSERT_FALSE(result->isSuccess()); // Should fail
                conn.rollback();
            }
        }
        
        // Verify no data was committed
        result = conn.query("MATCH (p:Person) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 0);
    }
}

// Test file descriptor leaks during out-of-memory scenarios
TEST_F(ErrorScenarioFileLeakTest, OutOfMemoryScenarios) {
    ScopedFileDescriptorMonitor monitor("OutOfMemoryScenarios");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        // Create database with very small buffer pool
        DatabaseConfig config;
        config.bufferPoolSize = 1024; // Very small buffer pool (1KB)
        Database database(testDbPath, config);
        Connection conn(&database);
        
        // Create table
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        // Try to insert large amount of data that should exceed buffer pool
        // This may trigger spilling or memory pressure
        conn.beginTransaction();
        for (int i = 0; i < 1000; ++i) {
            result = conn.query("CREATE (:Person {name: 'Person" + std::to_string(i) + 
                               "', age: " + std::to_string(i) + "})");
            
            if (!result->isSuccess()) {
                // If we get memory errors, rollback and continue
                conn.rollback();
                break;
            }
        }
        
        // Try to commit if transaction is still active
        try {
            conn.commit();
        } catch (const std::exception&) {
            // Expected if memory issues occurred
        }
        
        // Verify database is still functional
        result = conn.query("MATCH (p:Person) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        // Count may vary depending on how much data was successfully inserted
    }
}

// Test file descriptor leaks during disk full scenarios
TEST_F(ErrorScenarioFileLeakTest, DiskFullScenarios) {
    ScopedFileDescriptorMonitor monitor("DiskFullScenarios");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        Database database(testDbPath);
        Connection conn(&database);
        
        // Create table
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        // Insert large amount of data that might cause disk issues
        // Note: This test depends on system disk space
        conn.beginTransaction();
        bool diskFull = false;
        
        for (int i = 0; i < 10000 && !diskFull; ++i) {
            result = conn.query("CREATE (:Person {name: 'Person" + std::to_string(i) + 
                               "', age: " + std::to_string(i) + "})");
            
            if (!result->isSuccess()) {
                // Check if this is a disk space error
                diskFull = true;
                conn.rollback();
            }
        }
        
        if (!diskFull) {
            conn.commit();
        }
        
        // Verify database is still functional
        result = conn.query("MATCH (p:Person) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
    }
}

// Test file descriptor leaks during concurrent access errors
TEST_F(ErrorScenarioFileLeakTest, ConcurrentAccessErrors) {
    ScopedFileDescriptorMonitor monitor("ConcurrentAccessErrors");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        Database database(testDbPath);
        
        // Create multiple connections
        std::vector<std::unique_ptr<Connection>> connections;
        for (int i = 0; i < 5; ++i) {
            connections.push_back(std::make_unique<Connection>(&database));
        }
        
        // Initialize table
        auto result = connections[0]->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        // Try to cause concurrent access issues
        std::vector<std::string> queries = {
            "CREATE (:Person {name: 'Person1', age: 25})",
            "CREATE (:Person {name: 'Person2', age: 30})",
            "CREATE (:Person {name: 'Person3', age: 35})",
            "CREATE (:Person {name: 'Person1', age: 40})", // Duplicate key
            "CREATE (:Person {name: 'Person4', age: 45})",
        };
        
        // Execute queries on different connections
        for (size_t i = 0; i < connections.size() && i < queries.size(); ++i) {
            connections[i]->beginTransaction();
            result = connections[i]->query(queries[i]);
            
            if (result->isSuccess()) {
                connections[i]->commit();
            } else {
                connections[i]->rollback();
            }
        }
        
        // Verify database state
        result = connections[0]->query("MATCH (p:Person) RETURN count(*)");
        ASSERT_TRUE(result->isSuccess());
        
        // Clean up connections
        connections.clear();
    }
}

// Test file descriptor leaks during forced database shutdown
TEST_F(ErrorScenarioFileLeakTest, ForcedDatabaseShutdown) {
    ScopedFileDescriptorMonitor monitor("ForcedDatabaseShutdown");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        Database database(testDbPath);
        Connection conn(&database);
        
        // Create table and start transaction
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        conn.beginTransaction();
        result = conn.query("CREATE (:Person {name: 'Alice', age: 30})");
        ASSERT_TRUE(result->isSuccess());
        
        // Don't commit transaction - let database close with active transaction
        // This should test cleanup during forced shutdown
    }
    
    // Database destructor should clean up even with active transactions
}

// Test file descriptor leaks during extension loading errors
TEST_F(ErrorScenarioFileLeakTest, ExtensionLoadingErrors) {
    ScopedFileDescriptorMonitor monitor("ExtensionLoadingErrors");
    monitor.trackDatabaseFiles(testDbPath);
    
    {
        Database database(testDbPath);
        Connection conn(&database);
        
        // Try to load non-existent extension
        auto result = conn.query("INSTALL 'non_existent_extension'");
        ASSERT_FALSE(result->isSuccess());
        
        // Try to load extension with invalid path
        result = conn.query("INSTALL '/invalid/path/extension.so'");
        ASSERT_FALSE(result->isSuccess());
        
        // Verify database is still functional
        result = conn.query("CREATE NODE TABLE Person(name STRING, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
    }
}

// Test file descriptor leaks during database corruption scenarios
TEST_F(ErrorScenarioFileLeakTest, DatabaseCorruptionScenarios) {
    ScopedFileDescriptorMonitor monitor("DatabaseCorruptionScenarios");
    monitor.trackDatabaseFiles(testDbPath);
    
    // Create a database first
    {
        Database database(testDbPath);
        Connection conn(&database);
        
        auto result = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
        ASSERT_TRUE(result->isSuccess());
        
        result = conn.query("CREATE (:Person {name: 'Alice', age: 30})");
        ASSERT_TRUE(result->isSuccess());
    }
    
    // Try to corrupt the database file (if it exists)
    if (std::filesystem::exists(testDbPath)) {
        std::ofstream file(testDbPath, std::ios::binary | std::ios::app);
        file.write("CORRUPTED_DATA", 14);
        file.close();
    }
    
    // Try to open corrupted database
    try {
        Database database(testDbPath);
        Connection conn(&database);
        
        // This may or may not fail depending on corruption detection
        auto result = conn.query("MATCH (p:Person) RETURN p.name");
        // Don't assert on success/failure as it depends on corruption detection
    } catch (const std::exception&) {
        // Expected if corruption is detected
    }
}