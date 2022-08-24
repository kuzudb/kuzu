#include "test/test_utility/include/test_helper.h"

using ::testing::Test;
using namespace graphflow::testing;

class TinySnbReadTest : public BaseGraphLoadingTest {
public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }

    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        systemConfig->largePageBufferPoolSize = (1ull << 23);
        createDBAndConn();
    }

    inline void runTest(const string& queryFile) {
        auto queryConfigs = TestHelper::parseTestFile(queryFile);
        ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
    }
};

TEST_F(TinySnbReadTest, play) {
    auto b = 1;
    auto query = "MATCH (a:person)-[e1:knows]->(b:person)-[e2:knows]->(c:person), (a)-[e3:knows]->(c) RETURN COUNT(*)";
    auto p = conn->getTrianglePlan(query);
    auto s = p->getLastOperator()->toString();
    auto r = conn->executePlan(move(p));
    r->getQuerySummary()->printPlanToStdOut();
    auto rStr = TestHelper::convertResultToString(*r, false);
    auto k =1 ;
}

TEST_F(TinySnbReadTest, MatchExecute) {
    runTest("test/test_files/tinySNB/match/node.test");
    runTest("test/test_files/tinySNB/match/one_hop.test");
    runTest("test/test_files/tinySNB/match/two_hop.test");
    runTest("test/test_files/tinySNB/match/three_hop.test");
    runTest("test/test_files/tinySNB/match/four_hop.test");
    runTest("test/test_files/tinySNB/match/multi_query_part.test");
}

TEST_F(TinySnbReadTest, Filter) {
    runTest("test/test_files/tinySNB/filter/node.test");
    runTest("test/test_files/tinySNB/filter/one_hop.test");
    runTest("test/test_files/tinySNB/filter/two_hop.test");
    runTest("test/test_files/tinySNB/filter/four_hop.test");
    runTest("test/test_files/tinySNB/filter/five_hop.test");
    runTest("test/test_files/tinySNB/filter/multi_query_part.test");
}

TEST_F(TinySnbReadTest, Function) {
    runTest("test/test_files/tinySNB/function/date.test");
    runTest("test/test_files/tinySNB/function/timestamp.test");
    runTest("test/test_files/tinySNB/function/interval.test");
    runTest("test/test_files/tinySNB/function/list.test");
    runTest("test/test_files/tinySNB/function/arithmetic.test");
    runTest("test/test_files/tinySNB/function/boolean.test");
    runTest("test/test_files/tinySNB/function/string.test");
    runTest("test/test_files/tinySNB/function/cast.test");
}

TEST_F(TinySnbReadTest, Agg) {
    runTest("test/test_files/tinySNB/agg/simple.test");
    runTest("test/test_files/tinySNB/agg/hash.test");
    runTest("test/test_files/tinySNB/agg/distinct_agg.test");
    runTest("test/test_files/tinySNB/agg/multi_query_part.test");
}

TEST_F(TinySnbReadTest, Projection) {
    runTest("test/test_files/tinySNB/projection/projection.test");
    runTest("test/test_files/tinySNB/projection/skip_limit.test");
    runTest("test/test_files/tinySNB/projection/multi_query_part.test");
}

TEST_F(TinySnbReadTest, Subquery) {
    runTest("test/test_files/tinySNB/subquery/subquery.test");
}

TEST_F(TinySnbReadTest, OptionalMatch) {
    runTest("test/test_files/tinySNB/optional_match/optional_match.test");
}

TEST_F(TinySnbReadTest, OrderBy) {
    auto queryConfigs = TestHelper::parseTestFile(
        "test/test_files/tinySNB/order_by/order_by.test", true /* checkOutputOrder */);
    for (auto& queryConfig : queryConfigs) {
        queryConfig->checkOutputOrder = true;
    }
    ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
}

TEST_F(TinySnbReadTest, Union) {
    runTest("test/test_files/tinySNB/union/union.test");
}

// TODO(Ziyi): variable length join consumes way too much memory than it should.
// TEST_F(TinySnbReadTest, VarLengthAdjListExtendTests) {
//    vector<TestQueryConfig> queryConfigs;
//    queryConfigs = TestHelper::parseTestFile(
//        "test/runner/queries/var_length_extend/var_length_adj_list_extend.test");
//    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *conn));
//    queryConfigs = TestHelper::parseTestFile(
//        "test/runner/queries/var_length_extend/var_length_column_extend.test");
//    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *conn));
//}
