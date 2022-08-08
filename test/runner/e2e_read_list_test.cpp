#include <filesystem>

#include "gtest/gtest.h"
#include "test/test_utility/include/test_helper.h"

using ::testing::Test;
using namespace graphflow::testing;

class EndToEndReadLists2BytesPerEdgeTest : public DBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/read-list-tests/2-bytes-per-edge/"; }
};

class EndToEndReadLists4BytesPerEdgeTest : public DBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/read-list-tests/4-bytes-per-edge/"; }
};

class EndToEndReadListsSubQueryTest : public DBLoadedTest {
public:
    string getInputCSVDir() override {
        return "dataset/read-list-tests/large-list-sub-query-tests/";
    }
};

TEST_F(EndToEndReadLists2BytesPerEdgeTest, AdjLists2BytesPerEdgeTest) {
    auto queryConfigs =
        TestHelper::parseTestFile("test/test_files/read_list/2-bytes-per-edge.test");
    ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
}

TEST_F(EndToEndReadLists4BytesPerEdgeTest, AdjLists4BytesPerEdgeTest) {
    auto queryConfigs =
        TestHelper::parseTestFile("test/test_files/read_list/4-bytes-per-edge.test");
    ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
}

TEST_F(EndToEndReadLists2BytesPerEdgeTest, PropLists4BytesPerEdgeTest) {
    auto queryConfigs = TestHelper::parseTestFile(
        "test/test_files/read_list/small-large-property-list-reading.test");
    ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
}

// TODO(Ziyi): variable length join consumes way too much memory than it should.
// TEST_F(EndToEndReadLists2BytesPerEdgeTest, VarLengthExtendLargeAdjListTest) {
//    auto queryConfigs = TestHelper::parseTestFile(
//        "test/test_files/read_list/var_length_large_adj_list_extend.test");
//    ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
//}

TEST_F(EndToEndReadListsSubQueryTest, LargeListSubQueryTest) {
    auto queryConfigs =
        TestHelper::parseTestFile("test/test_files/read_list/large-list-sub-query.test");
    ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
}
