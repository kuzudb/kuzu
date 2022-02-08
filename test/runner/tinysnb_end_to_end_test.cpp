#include <filesystem>

#include "gtest/gtest.h"
#include "test/test_utility/include/test_helper.h"

using ::testing::Test;
using namespace graphflow::testing;

class TinySnbProcessorTest : public DBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }
};

TEST_F(TinySnbProcessorTest, StructuralQueries) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/structural/nodes.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/structural/paths.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/structural/stars.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/structural/multi_query.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, FilteredQueries) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/filtered/id_comparison.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/filtered/nodes.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/filtered/paths.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/filtered/stars.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/filtered/str_operations.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs =
        TestHelper::parseTestFile("test/runner/queries/filtered/unstructured_properties.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/filtered/multi_query.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, FrontierQueries) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/structural/frontier.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, DateDataTypeTests) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/data_types/date_data_type.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, TimestampDataTypeTests) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs =
        TestHelper::parseTestFile("test/runner/queries/data_types/timestamp_data_type.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, IntervalDataTypeTests) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs =
        TestHelper::parseTestFile("test/runner/queries/data_types/interval_data_type.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, AggregateTests) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs =
        TestHelper::parseTestFile("test/runner/queries/aggregate/distinct_aggregate.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/aggregate/distinct.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/aggregate/hash_aggregate.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/aggregate/simple_aggregate.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/aggregate/multi_query.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, ProjectionTests) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/projection/projection.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/projection/with.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/projection/skip_limit.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, SubqueryTests) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/subquery/subquery.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, OptionalMatchTests) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/optional/optional_match.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, OrderByTests) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs = TestHelper::parseTestFile(
        "test/runner/queries/order_by/order_by_tiny_snb.test", true /* checkOutputOrder */);
    for (auto& queryConfig : queryConfigs) {
        queryConfig.checkOutputOrder = true;
    }
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, UnionAllTests) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/union/union_tiny_snb.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}
