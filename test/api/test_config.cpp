#include "test/test_utility/include/test_helper.h"

#include "src/main/include/graphflowdb.h"

using namespace graphflow::testing;
using namespace graphflow::main;

TEST_F(ApiTest, database_config) {
    auto db = make_unique<Database>(DatabaseConfig(TEMP_TEST_DIR));
    ASSERT_NO_THROW(db->resizeBufferManager(StorageConfig::DEFAULT_BUFFER_POOL_SIZE * 2));
    ASSERT_EQ(db->getDefaultBMSize(), StorageConfig::DEFAULT_BUFFER_POOL_SIZE * 2);
    ASSERT_EQ(db->getLargeBMSize(), StorageConfig::DEFAULT_BUFFER_POOL_SIZE * 2);
}

TEST_F(ApiTest, client_config) {
    auto db = make_unique<Database>(DatabaseConfig(TEMP_TEST_DIR));
    auto conn = make_unique<Connection>(db.get());
    ASSERT_NO_THROW(conn->setMaxNumThreadForExec(2));
    ASSERT_EQ(conn->getMaxNumThreadForExec(), 2);
}
