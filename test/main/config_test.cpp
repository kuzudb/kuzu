#include "main_test_helper/main_test_helper.h"

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::testing;

TEST_F(ApiTest, ClientConfig) {
    ASSERT_NO_THROW(conn->setMaxNumThreadForExec(2));
    ASSERT_EQ(conn->getMaxNumThreadForExec(), 2);
}

TEST_F(ApiTest, DatabasePathIncorrect) {
    try {
        std::make_unique<Database>("0:* /? \" < > |");
        FAIL();
    } catch (Exception& e) {
        ASSERT_TRUE(std::string(e.what()).find("Failed to create directory") != std::string::npos);
    }
}
