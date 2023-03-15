#include "main_test_helper/main_test_helper.h"

using namespace kuzu::common;
using namespace kuzu::testing;

TEST_F(ApiTest, ClientConfig) {
    spdlog::set_level(spdlog::level::debug);
    ASSERT_NO_THROW(conn->setMaxNumThreadForExec(2));
    ASSERT_EQ(conn->getMaxNumThreadForExec(), 2);
}

TEST_F(ApiTest, DatabasePathIncorrect) {
    spdlog::set_level(spdlog::level::debug);
    try {
        std::make_unique<Database>("0:* /? \" < > |");
        FAIL();
    } catch (Exception& e) {
        ASSERT_TRUE(std::string(e.what()).find("Failed to create directory") != std::string::npos);
    }
}
