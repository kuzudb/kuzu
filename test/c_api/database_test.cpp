#include "c_api_test/c_api_test.h"

class CApiDatabaseTest : public APIEmptyDBTest {
public:
    void SetUp() override { APIEmptyDBTest::SetUp(); }
};

TEST_F(CApiDatabaseTest, CreationAndDestroy) {
    auto databasePathCStr = databasePath.c_str();
    auto database = kuzu_database_init(databasePathCStr, kuzu_default_system_config());
    ASSERT_NE(database, nullptr);
    ASSERT_NE(database->_database, nullptr);
    auto databaseCpp = static_cast<Database*>(database->_database);
    ASSERT_NE(databaseCpp, nullptr);
    kuzu_database_destroy(database);
}

TEST_F(CApiDatabaseTest, CreationInvalidPath) {
    auto databasePathCStr = (char*)"";
    auto database = kuzu_database_init(databasePathCStr, kuzu_default_system_config());
    ASSERT_EQ(database, nullptr);
}
