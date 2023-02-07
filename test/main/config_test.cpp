#include "main_test_helper/main_test_helper.h"

using namespace kuzu::common;
using namespace kuzu::testing;

TEST_F(ApiTest, DatabaseConfig) {
    spdlog::set_level(spdlog::level::debug);
    ASSERT_NO_THROW(database->resizeBufferManager(StorageConfig::DEFAULT_BUFFER_POOL_SIZE * 2));
    ASSERT_EQ(getDefaultBMSize(*database), (uint64_t)(StorageConfig::DEFAULT_BUFFER_POOL_SIZE * 2 *
                                                      StorageConfig::DEFAULT_PAGES_BUFFER_RATIO));
    ASSERT_EQ(getLargeBMSize(*database), (uint64_t)(StorageConfig::DEFAULT_BUFFER_POOL_SIZE * 2 *
                                                    StorageConfig::LARGE_PAGES_BUFFER_RATIO));
}

TEST_F(ApiTest, ClientConfig) {
    spdlog::set_level(spdlog::level::debug);
    ASSERT_NO_THROW(conn->setMaxNumThreadForExec(2));
    ASSERT_EQ(conn->getMaxNumThreadForExec(), 2);
}

TEST_F(ApiTest, DatabasePathIncorrect) {
    spdlog::set_level(spdlog::level::debug);
    try {
        std::make_unique<Database>(DatabaseConfig("/\\0:* /? \" < > |"));
        FAIL();
    } catch (Exception& e) {
        ASSERT_TRUE(std::string(e.what()).find("Failed to create directory") != std::string::npos);
    }
}
