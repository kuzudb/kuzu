#include "test_helper/test_helper.h"

using ::testing::Test;
using namespace kuzu::testing;

class LongStringPKTest : public DBTest {
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/long-string-pk-tests/");
    }
};

class TinySnbReadTest : public DBTest {
public:
    string getInputCSVDir() override { return TestHelper::appendKuzuRootPath("dataset/tinysnb/"); }
};

TEST_F(LongStringPKTest, LongStringPKTest) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/long_string_pk/long_string_pk.test"));
}

TEST_F(TinySnbReadTest, MatchExecute) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/match/node.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/match/one_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/match/two_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/match/three_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/match/four_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/match/multi_query_part.test"));
}

TEST_F(TinySnbReadTest, Filter) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/filter/node.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/filter/one_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/filter/two_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/filter/four_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/filter/five_hop.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/filter/multi_query_part.test"));
}

TEST_F(TinySnbReadTest, Function) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/function/date.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/function/timestamp.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/function/interval.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/function/list.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/function/arithmetic.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/function/boolean.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/function/string.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/function/cast.test"));
}

TEST_F(TinySnbReadTest, Agg) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/agg/simple.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/agg/hash.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/agg/distinct_agg.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/agg/multi_query_part.test"));
}

TEST_F(TinySnbReadTest, Cyclic) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/cyclic/cyclic.test"));
}

TEST_F(TinySnbReadTest, Projection) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/projection/projection.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/projection/skip_limit.test"));
    runTest(
        TestHelper::appendKuzuRootPath("test/test_files/tinySNB/projection/multi_query_part.test"));
}

TEST_F(TinySnbReadTest, Subquery) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/subquery/exists.test"));
}

TEST_F(TinySnbReadTest, OptionalMatch) {
    runTest(TestHelper::appendKuzuRootPath(
        "test/test_files/tinySNB/optional_match/optional_match.test"));
}

TEST_F(TinySnbReadTest, OrderBy) {
    auto queryConfigs = TestHelper::parseTestFile(
        TestHelper::appendKuzuRootPath("test/test_files/tinySNB/order_by/order_by.test"),
        true /* checkOutputOrder */);
    for (auto& queryConfig : queryConfigs) {
        queryConfig->checkOutputOrder = true;
    }
    ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
}

TEST_F(TinySnbReadTest, Union) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/union/union.test"));
}

TEST_F(TinySnbReadTest, Unwind) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/tinySNB/unwind/unwind.test"));
}

// TEST_F(TinySnbReadTest, VarLengthExtendTests) {
//    runTest(TestHelper::appendKuzuRootPath(
//        "test/test_files/tinySNB/var_length_extend/var_length_adj_list_extend.test"));
//    runTest(TestHelper::appendKuzuRootPath(
//        "test/test_files/tinySNB/var_length_extend/var_length_column_extend.test"));
//}
