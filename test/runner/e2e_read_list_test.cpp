#include <filesystem>

#include "graph_test/graph_test.h"

using ::testing::Test;
using namespace kuzu::testing;

class EndToEndReadLists2BytesPerEdgeTest : public DBTest {
public:
    string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/read-list-tests/2-bytes-per-edge/");
    }
};

class EndToEndReadLists4BytesPerEdgeTest : public DBTest {
public:
    string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/read-list-tests/4-bytes-per-edge/");
    }
};

class EndToEndReadListsSubQueryTest : public DBTest {
public:
    string getInputDir() override {
        return TestHelper::appendKuzuRootPath(
            "dataset/read-list-tests/large-list-sub-query-tests/");
    }
};

TEST_F(EndToEndReadLists2BytesPerEdgeTest, AdjLists2BytesPerEdgeTest) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/read_list/2-bytes-per-edge.test"));
}

TEST_F(EndToEndReadLists4BytesPerEdgeTest, AdjLists4BytesPerEdgeTest) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/read_list/4-bytes-per-edge.test"));
}

TEST_F(EndToEndReadLists2BytesPerEdgeTest, PropLists4BytesPerEdgeTest) {
    runTest(TestHelper::appendKuzuRootPath(
        "test/test_files/read_list/small-large-property-list-reading.test"));
}

TEST_F(EndToEndReadLists2BytesPerEdgeTest, VarLengthExtendLargeAdjListTest) {
    runTest(TestHelper::appendKuzuRootPath(
        "test/test_files/read_list/var_length_large_adj_list_extend.test"));
}

// TEST_F(EndToEndReadListsSubQueryTest, LargeListSubQueryTest) {
//    auto queryConfigs =
//        TestHelper::parseTestFile("test/test_files/read_list/large-list-sub-query.test");
//    ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
//}
