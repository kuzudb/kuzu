#include <filesystem>

#include "graph_test/graph_test.h"

using ::testing::Test;
using namespace kuzu::testing;

class EndToEndReadLargeListsTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/read-list-tests/large-list/");
    }
};

class EndToEndReadListsSubQueryTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath(
            "dataset/read-list-tests/large-list-sub-query-tests/");
    }
};

TEST_F(EndToEndReadLargeListsTest, LargeAdjListTest) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/read_list/large-list.test"));
}

TEST_F(EndToEndReadLargeListsTest, PropLists4BytesPerEdgeTest) {
    runTest(TestHelper::appendKuzuRootPath(
        "test/test_files/read_list/small-large-property-list-reading.test"));
}

TEST_F(EndToEndReadLargeListsTest, VarLengthExtendLargeAdjListTest) {
    runTest(TestHelper::appendKuzuRootPath(
        "test/test_files/read_list/var_length_large_adj_list_extend.test"));
}

// TEST_F(EndToEndReadListsSubQueryTest, LargeListSubQueryTest) {
//    auto queryConfigs =
//        TestHelper::parseTestFile("test/test_files/read_list/large-list-sub-query.test");
//    ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
//}
