#include "main_test_helper/main_test_helper.h"

using namespace kuzu::common;
using namespace kuzu::testing;

class ReadOnlyTest : public ApiTest {};

TEST_F(ReadOnlyTest, Test) {
    ASSERT_TRUE(conn->query("CALL PROJECT_GRAPH('proj', ['person'], ['knows'])")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE Test (id INT64 PRIMARY KEY, arr INT64[4])")->isSuccess());
    systemConfig->readOnly = true;
    createDBAndConn();
    ASSERT_STREQ("Connection exception: Cannot execute write operations in a read-only database!",
        conn->query("CALL CLEAR_WARNINGS()")->toString().c_str());
    ASSERT_STREQ("Connection exception: Cannot execute write operations in a read-only database!",
        conn->query("CALL PROJECT_GRAPH('proj2', ['person'], ['knows'])")->toString().c_str());
    ASSERT_STREQ("Connection exception: Cannot execute write operations in a read-only database!",
        conn->query("CALL DROP_PROJECTED_GRAPH('proj')")->toString().c_str());
    ASSERT_STREQ("Connection exception: Cannot execute write operations in a read-only database!",
        conn->query("CALL _CACHE_ARRAY_COLUMN_LOCALLY('Test', 'arr')")->toString().c_str());
}
