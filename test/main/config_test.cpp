#include "include/main_test_helper.h"

TEST_F(ApiTest, DatabaseConfig) {
    auto db = make_unique<Database>(DatabaseConfig(TestHelper::TEMP_TEST_DIR));
    ASSERT_NO_THROW(db->resizeBufferManager(StorageConfig::DEFAULT_BUFFER_POOL_SIZE * 2));
    ASSERT_EQ(db->getDefaultBMSize(), StorageConfig::DEFAULT_BUFFER_POOL_SIZE * 2);
    ASSERT_EQ(db->getLargeBMSize(), StorageConfig::DEFAULT_BUFFER_POOL_SIZE * 2);
}

TEST_F(ApiTest, ClientConfig) {
    auto db = make_unique<Database>(DatabaseConfig(TestHelper::TEMP_TEST_DIR));
    auto conn = make_unique<Connection>(db.get());
    ASSERT_NO_THROW(conn->setMaxNumThreadForExec(2));
    ASSERT_EQ(conn->getMaxNumThreadForExec(), 2);
}
