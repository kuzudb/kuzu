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

    // The query parameter has to be a literal/parameter expression.
    prepared = conn->prepare("MATCH (d:person) CALL QUERY_FTS_INDEX('person', 'personIdx', "
                             "d.fName) RETURN _node.ID, score;");
    auto result = conn->execute(prepared.get());
    ASSERT_FALSE(result->isSuccess());
    ASSERT_EQ(result->getErrorMessage(),
        "Runtime exception: The query must be a parameter/literal expression.");

    // User must have to give a value when executing the QUERY_FTS_INDEX.
    prepared =
        conn->prepare("CALL QUERY_FTS_INDEX('person', 'personIdx', $1) RETURN _node.ID, score;");
    result = conn->execute(prepared.get());
    ASSERT_FALSE(result->isSuccess());
    ASSERT_EQ(result->getErrorMessage(),
        "Runtime exception: The query is a parameter expression. Please assign it a value.");

    // Table name can't be a parameter expression.
    prepared =
        conn->prepare("CALL QUERY_FTS_INDEX($3, 'personIdx', 'alice') RETURN _node.ID, score;");
    result = conn->execute(prepared.get(), std::make_pair(std::string("3"), std::string("person")));
    ASSERT_FALSE(result->isSuccess());
    ASSERT_EQ(result->getErrorMessage(),
        "Binder exception: The table and index name must be literal expressions.");
}

} // namespace testing
} // namespace kuzu
