#include "main_test_helper/main_test_helper.h"

using namespace kuzu::common;
using namespace kuzu::testing;
using namespace kuzu::main;

class AccessModeTest : public ApiTest {};

TEST_F(AccessModeTest, testReadWrite) {
    systemConfig->accessMode = AccessMode::READ_WRITE;
    auto db = std::make_unique<Database>(databasePath, *systemConfig);
    auto con = std::make_unique<Connection>(db.get());
    ASSERT_TRUE(con->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))")
                    ->isSuccess());
    ASSERT_TRUE(con->query("CREATE (:Person {name: 'Alice', age: 25})")->isSuccess());
    ASSERT_TRUE(con->query("MATCH (:Person) RETURN COUNT(*)")->isSuccess());
    db.reset();
    systemConfig->accessMode = AccessMode::READ_ONLY;
    std::unique_ptr<Database> db2;
    std::unique_ptr<Connection> con2;
    EXPECT_NO_THROW(db2 = std::make_unique<Database>(databasePath, *systemConfig));
    EXPECT_NO_THROW(con2 = std::make_unique<Connection>(db2.get()));
    EXPECT_ANY_THROW(con2->query("DROP TABLE Person"));
    EXPECT_NO_THROW(con2->query("MATCH (:Person) RETURN COUNT(*)"));
}
