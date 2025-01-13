#include "main_test_helper/main_test_helper.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

TEST_F(ApiTest, PrepareFTSTest) {
    createDBAndConn();
    ASSERT_TRUE(conn->query(common::stringFormat("LOAD EXTENSION '{}'",
                                TestHelper::appendKuzuRootPath(
                                    "extension/fts/build/libfts.kuzu_extension")))
                    ->isSuccess());
    ASSERT_TRUE(
        conn->query("CALL CREATE_FTS_INDEX('person', 'personIdx', ['fName'])")->isSuccess());
    auto prepared =
        conn->prepare("CALL QUERY_FTS_INDEX('person', 'personIdx', $q) RETURN _node.ID, score;");
    auto preparedResult = TestHelper::convertResultToString(
        *conn->execute(prepared.get(), std::make_pair(std::string("q"), std::string("alice"))));
    auto nonPreparedResult = TestHelper::convertResultToString(*conn->query(
        "CALL QUERY_FTS_INDEX('person', 'personIdx', 'alice') RETURN _node.ID, score;"));
    sortAndCheckTestResults(preparedResult, nonPreparedResult);
}

} // namespace testing
} // namespace kuzu
