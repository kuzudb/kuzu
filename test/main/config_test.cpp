#include "main_test_helper/main_test_helper.h"

using namespace kuzu::testing;

TEST_F(ApiTest, DatabaseConfig) {
    auto db = make_unique<Database>(DatabaseConfig(TestHelper::getTmpTestDir()));
    spdlog::set_level(spdlog::level::debug);
    ASSERT_NO_THROW(db->resizeBufferManager(StorageConfig::DEFAULT_BUFFER_POOL_SIZE * 2));
    ASSERT_EQ(getDefaultBMSize(*db), (uint64_t)(StorageConfig::DEFAULT_BUFFER_POOL_SIZE * 2 *
                                                StorageConfig::DEFAULT_PAGES_BUFFER_RATIO));
    ASSERT_EQ(getLargeBMSize(*db), (uint64_t)(StorageConfig::DEFAULT_BUFFER_POOL_SIZE * 2 *
                                              StorageConfig::LARGE_PAGES_BUFFER_RATIO));
}

TEST_F(ApiTest, ClientConfig) {
    auto db = make_unique<Database>(DatabaseConfig(TestHelper::getTmpTestDir()));
    spdlog::set_level(spdlog::level::debug);
    auto conn = make_unique<Connection>(db.get());
    ASSERT_NO_THROW(conn->setMaxNumThreadForExec(2));
    ASSERT_EQ(conn->getMaxNumThreadForExec(), 2);
}
