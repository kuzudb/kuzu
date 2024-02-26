#include "common/exception/buffer_manager.h"
#include "main_test_helper/main_test_helper.h"

using namespace kuzu::common;
using namespace kuzu::testing;
using namespace kuzu::main;

class SystemConfigTest : public ApiTest {};

void assertQuery(QueryResult& result) {
    auto a = result.toString();
    ASSERT_TRUE(result.isSuccess()) << result.toString();
}

TEST_F(SystemConfigTest, testAccessMode) {
    systemConfig->readOnly = false;
    auto db = std::make_unique<Database>(databasePath, *systemConfig);
    auto con = std::make_unique<Connection>(db.get());
    assertQuery(
        *con->query("CREATE NODE TABLE Person1(name STRING, age INT64, PRIMARY KEY(name))"));
    assertQuery(*con->query("CREATE (:Person1 {name: 'Alice', age: 25})"));
    assertQuery(*con->query("MATCH (:Person1) RETURN COUNT(*)"));
    db.reset();
    systemConfig->readOnly = true;
    std::unique_ptr<Database> db2;
    std::unique_ptr<Connection> con2;
    EXPECT_NO_THROW(db2 = std::make_unique<Database>(databasePath, *systemConfig));
    EXPECT_NO_THROW(con2 = std::make_unique<Connection>(db2.get()));
    ASSERT_FALSE(con2->query("DROP TABLE Person")->isSuccess());
    EXPECT_NO_THROW(con2->query("MATCH (:Person) RETURN COUNT(*)"));
}

TEST_F(SystemConfigTest, testMaxDBSize) {
    systemConfig->maxDBSize = 1024;
    try {
        auto db = std::make_unique<Database>(databasePath, *systemConfig);
    } catch (BufferManagerException e) {
        ASSERT_EQ(std::string(e.what()),
            "Buffer manager exception: The given max db size should be at least 4194304 bytes.");
    }
    systemConfig->maxDBSize = 4194305;
    try {
        auto db = std::make_unique<Database>(databasePath, *systemConfig);
    } catch (BufferManagerException e) {
        ASSERT_EQ(std::string(e.what()),
            "Buffer manager exception: The given max db size should be a power of 2.");
    }
    systemConfig->maxDBSize = 4194304;
    try {
        auto db = std::make_unique<Database>(databasePath, *systemConfig);
    } catch (BufferManagerException e) {
        ASSERT_EQ(std::string(e.what()),
            "Buffer manager exception: No more frame groups can be added to the allocator.");
    }
    systemConfig->maxDBSize = 1ull << 30;
    EXPECT_NO_THROW(auto db = std::make_unique<Database>(databasePath, *systemConfig));
}

TEST_F(SystemConfigTest, testBufferPoolSize) {
    systemConfig->bufferPoolSize = 1024;
    try {
        auto db = std::make_unique<Database>(databasePath, *systemConfig);
    } catch (BufferManagerException e) {
        ASSERT_EQ(std::string(e.what()),
            "Buffer manager exception: The given buffer pool size should be at least 4KB.");
    }
    systemConfig->bufferPoolSize = BufferPoolConstants::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING;
    EXPECT_NO_THROW(auto db = std::make_unique<Database>(databasePath, *systemConfig));
}
