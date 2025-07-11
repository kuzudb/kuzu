#include "api_test/api_test.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

TEST_F(ApiTest, ReadOnlyDBTest) {
    if (inMemMode) {
        GTEST_SKIP();
    }
    createDBAndConn();
#ifndef __STATIC_LINK_EXTENSION_TEST__
    ASSERT_TRUE(conn->query(common::stringFormat("LOAD EXTENSION '{}'",
                                TestHelper::appendKuzuRootPath(
                                    "extension/fts/build/libfts.kuzu_extension")))
                    ->isSuccess());
#endif
    ASSERT_TRUE(conn->query("CREATE NODE TABLE doc (NAME STRING, PRIMARY KEY(NAME))")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (d:doc {NAME: 'alice'})")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (d:doc {NAME: 'bob'})")->isSuccess());
    ASSERT_TRUE(conn->query("CALL CREATE_FTS_INDEX('doc', 'docIdx', ['NAME'])")->isSuccess());
    systemConfig->readOnly = true;
    createDBAndConn();
#ifndef __STATIC_LINK_EXTENSION_TEST__
    ASSERT_TRUE(conn->query(common::stringFormat("LOAD EXTENSION '{}'",
                                TestHelper::appendKuzuRootPath(
                                    "extension/fts/build/libfts.kuzu_extension")))
                    ->isSuccess());
#endif
    ASSERT_STREQ("Connection exception: Cannot execute write operations in a read-only database!",
        conn->query("CALL CREATE_FTS_INDEX('doc', 'docIdx1', ['NAME'])")->toString().c_str());
    ASSERT_TRUE(
        conn->query("CALL QUERY_FTS_INDEX('doc', 'docIdx', 'alice') RETURN node.NAME, score")
            ->isSuccess());
}

} // namespace testing
} // namespace kuzu
