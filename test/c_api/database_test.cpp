#include <fstream>

#include "c_api/kuzu.h"
#include "common/exception/io.h"
#include "common/file_system/virtual_file_system.h"
#include "graph_test/api_graph_test.h"
#include "gtest/gtest.h"

using namespace kuzu::main;
using namespace kuzu::testing;

class CApiDatabaseTest : public APIEmptyDBTest {
public:
    void SetUp() override {
        APIEmptyDBTest::SetUp();
        defaultSystemConfig = kuzu_default_system_config();

        // limit memory usage by keeping max number of threads small
        defaultSystemConfig.max_num_threads = 2;
    }

    kuzu_system_config defaultSystemConfig;
};

TEST_F(CApiDatabaseTest, CreationAndDestroy) {
    kuzu_database database;
    kuzu_state state;
    auto databasePathCStr = databasePath.c_str();
    auto systemConfig = defaultSystemConfig;
    state = kuzu_database_init(databasePathCStr, systemConfig, &database);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(database._database, nullptr);
    auto databaseCpp = static_cast<Database*>(database._database);
    ASSERT_NE(databaseCpp, nullptr);
    kuzu_database_destroy(&database);
}

TEST_F(CApiDatabaseTest, CreationReadOnly) {
    kuzu_database database;
    kuzu_connection connection;
    kuzu_query_result queryResult;
    kuzu_state state;
    auto databasePathCStr = databasePath.c_str();
    auto systemConfig = defaultSystemConfig;
    // First, create a read-write database.
    state = kuzu_database_init(databasePathCStr, systemConfig, &database);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(database._database, nullptr);
    auto databaseCpp = static_cast<Database*>(database._database);
    ASSERT_NE(databaseCpp, nullptr);
    kuzu_database_destroy(&database);
    // Now, access the same database read-only.
    systemConfig.read_only = true;
    state = kuzu_database_init(databasePathCStr, systemConfig, &database);
    if (databasePath == "" || databasePath == ":memory:") {
        ASSERT_EQ(state, KuzuError);
        ASSERT_EQ(database._database, nullptr);
        return;
    }
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(database._database, nullptr);
    databaseCpp = static_cast<Database*>(database._database);
    ASSERT_NE(databaseCpp, nullptr);
    // Try to write to the database.
    state = kuzu_connection_init(&database, &connection);
    ASSERT_EQ(state, KuzuSuccess);
    state = kuzu_connection_query(&connection,
        "CREATE NODE TABLE User(name STRING, age INT64, reg_date DATE, PRIMARY KEY (name))",
        &queryResult);
    ASSERT_EQ(state, KuzuError);
    ASSERT_FALSE(kuzu_query_result_is_success(&queryResult));
    kuzu_query_result_destroy(&queryResult);
    kuzu_connection_destroy(&connection);
    kuzu_database_destroy(&database);
}

TEST_F(CApiDatabaseTest, CreationInMemory) {
    kuzu_database database;
    kuzu_state state;
    auto databasePathCStr = (char*)"";
    state = kuzu_database_init(databasePathCStr, defaultSystemConfig, &database);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_database_destroy(&database);
    databasePathCStr = (char*)":memory:";
    state = kuzu_database_init(databasePathCStr, defaultSystemConfig, &database);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_database_destroy(&database);
}

#ifndef __WASM__ // home directory is not available in WASM
TEST_F(CApiDatabaseTest, CreationHomeDir) {
    kuzu_database database;
    kuzu_connection connection;
    kuzu_state state;
    auto databasePathCStr = (char*)"~/ku_test.db/";
    state = kuzu_database_init(databasePathCStr, defaultSystemConfig, &database);
    ASSERT_EQ(state, KuzuSuccess);
    state = kuzu_connection_init(&database, &connection);
    ASSERT_EQ(state, KuzuSuccess);
    auto homePath =
        getClientContext(*(Connection*)(connection._connection))->getClientConfig()->homeDirectory;
    kuzu_connection_destroy(&connection);
    kuzu_database_destroy(&database);
    std::filesystem::remove_all(homePath + "/ku_test.db");
}
#endif

TEST_F(CApiDatabaseTest, VirtualFileSystemDeleteFiles) {
    std::string homeDir = "/tmp/dbHome/";
    kuzu::common::VirtualFileSystem vfs(homeDir);
    std::filesystem::create_directories("/tmp/test1");
    std::filesystem::create_directories("/tmp/dbHome/test1");

    // Attempt to delete files outside the home directory (should error)
    try {
        vfs.removeFileIfExists("/tmp/test1");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path /tmp/test1 is not within the allowed "
                               "home directory /tmp/dbHome/");
    }

    vfs.removeFileIfExists("/tmp/dbHome/test1");

    ASSERT_TRUE(std::filesystem::exists("/tmp/test1"));
    ASSERT_FALSE(std::filesystem::exists("/tmp/dbHome/test1"));

    // Cleanup: Remove directories after the test
    std::filesystem::remove_all("/tmp/test1");
    std::filesystem::remove_all("/tmp/dbHome/test1");
}

#ifndef __WASM__ // home directory is not available in WASM
TEST_F(CApiDatabaseTest, VirtualFileSystemDeleteFilesWithHome) {
    std::string homeDir = "~/tmp/dbHome/";
    kuzu::common::VirtualFileSystem vfs(homeDir);
    std::filesystem::create_directories("~/tmp/test1");
    std::filesystem::create_directories("~/tmp/dbHome/test1");

    // Attempt to delete files outside the home directory (should error)
    try {
        vfs.removeFileIfExists("~/tmp/test1");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path ~/tmp/test1 is not within the allowed "
                               "home directory ~/tmp/dbHome/");
    }

    // Attempt to delete files outside the home directory (should error)
    try {
        vfs.removeFileIfExists("~");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(),
            "IO exception: Error: Path ~ is not within the allowed home directory ~/tmp/dbHome/");
    }

    vfs.removeFileIfExists("~/tmp/dbHome/test1");

    ASSERT_TRUE(std::filesystem::exists("~/tmp/test1"));
    ASSERT_FALSE(std::filesystem::exists("~/tmp/dbHome/test1"));

    // Cleanup: Remove directories after the test
    std::filesystem::remove_all("~/tmp/test1");
    std::filesystem::remove_all("~/tmp/dbHome/test1");
}
#endif

TEST_F(CApiDatabaseTest, VirtualFileSystemDeleteFilesEdgeCases) {
    std::string homeDir = "/tmp/dbHome/";
    kuzu::common::VirtualFileSystem vfs(homeDir);
    std::filesystem::create_directories("/tmp/dbHome/../test2");
    std::filesystem::create_directories("/tmp");
    std::filesystem::create_directories("/tmp/dbHome/test2");

    // Attempt to delete files outside the home directory (should error)
    try {
        vfs.removeFileIfExists("/tmp/dbHome/../test2");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path /tmp/dbHome/../test2 is not within the "
                               "allowed home directory /tmp/dbHome/");
    }

    try {
        vfs.removeFileIfExists("/tmp");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(),
            "IO exception: Error: Path /tmp is not within the allowed home directory /tmp/dbHome/");
    }

    try {
        vfs.removeFileIfExists("/tmp/");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path /tmp/ is not within the allowed home "
                               "directory /tmp/dbHome/");
    }

    try {
        vfs.removeFileIfExists("/tmp//////////////////");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path /tmp////////////////// is not within the "
                               "allowed home directory /tmp/dbHome/");
    }

    try {
        vfs.removeFileIfExists("/tmp/./.././");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(), "IO exception: Error: Path /tmp/./.././ is not within the allowed "
                               "home directory /tmp/dbHome/");
    }

    try {
        vfs.removeFileIfExists("/");
    } catch (const kuzu::common::IOException& e) {
        // Expected behavior
        EXPECT_STREQ(e.what(),
            "IO exception: Error: Path / is not within the allowed home directory /tmp/dbHome/");
    }

    vfs.removeFileIfExists("/tmp/dbHome/test2");

    ASSERT_TRUE(std::filesystem::exists("/tmp/test2"));
    ASSERT_FALSE(std::filesystem::exists("/tmp/dbHome/test2"));

    // Cleanup: Remove directories after the test
    std::filesystem::remove_all("/tmp/test2");
    std::filesystem::remove_all("/tmp/dbHome/test2");
}

#if defined(_WIN32)
TEST_F(CApiDatabaseTest, VirtualFileSystemDeleteFilesWindowsPaths) {
    // Test Home Directory
    std::string homeDir = "C:\\Desktop\\dir";
    kuzu::common::VirtualFileSystem vfs(homeDir);

    // Setup directories for testing
    std::filesystem::create_directories("C:\\test1");
    std::filesystem::create_directories("C:\\Desktop\\dir\\test1");

    // Mixed separators: HomeDir uses '\' while path uses '/'
    std::string mixedSeparatorPath = "C:\\Desktop/dir/test1";

    // Attempt to delete files outside the home directory (should error)
    try {
        vfs.removeFileIfExists("C:\\test1");
        FAIL() << "Expected exception for path outside home directory.";
    } catch (const kuzu::common::IOException& e) {
        EXPECT_STREQ(e.what(), "IO exception: Error: Path C:\\test1 is not within the allowed home "
                               "directory C:\\Desktop\\dir");
    }

    // Attempt to delete file inside the home directory with mixed separators (should succeed)
    try {
        vfs.removeFileIfExists(mixedSeparatorPath);
    } catch (const kuzu::common::IOException& e) {
        FAIL() << "Unexpected exception when deleting files: " << e.what();
    }

    ASSERT_FALSE(std::filesystem::exists("C:\\Desktop\\dir\\test1")); // Should be deleted

    // Cleanup
    std::filesystem::remove_all("C:\\test1");
    std::filesystem::remove_all("C:\\Desktop\\dir\\test1");
}
#endif

TEST_F(CApiDatabaseTest, VirtualFileSystemDeleteFilesWildcardNoRemoval) {
    // Test Home Directory
    std::string homeDir = "/tmp/dbHome_wildcard/";
    kuzu::common::VirtualFileSystem vfs(homeDir);

    // Setup files and directories
    std::filesystem::create_directories("/tmp/dbHome_wildcard/test1_wildcard");
    std::filesystem::create_directories("/tmp/dbHome_wildcard/test2_wildcard");
    std::filesystem::create_directories("/tmp/dbHome_wildcard/nested_wildcard");
    std::ofstream("/tmp/dbHome_wildcard/nested_wildcard/file1.txt").close();
    std::ofstream("/tmp/dbHome_wildcard/nested_wildcard/file2.test").close();

    // Attempt to remove files with wildcard pattern
    try {
        vfs.removeFileIfExists("/tmp/dbHome_wildcard/test*");
    } catch (const kuzu::common::IOException& e) {
        // Verify the exception is thrown for unsupported wildcard
        EXPECT_STREQ(e.what(), "Error: Wildcard patterns are not supported in paths.");
    }

    // Verify files and directories still exist
    ASSERT_TRUE(std::filesystem::exists("/tmp/dbHome_wildcard/test1_wildcard"));
    ASSERT_TRUE(std::filesystem::exists("/tmp/dbHome_wildcard/test2_wildcard"));
    ASSERT_TRUE(std::filesystem::exists("/tmp/dbHome_wildcard/nested_wildcard/file1.txt"));
    ASSERT_TRUE(std::filesystem::exists("/tmp/dbHome_wildcard/nested_wildcard/file2.test"));

    // Cleanup
    std::filesystem::remove_all("/tmp/dbHome_wildcard");
}
