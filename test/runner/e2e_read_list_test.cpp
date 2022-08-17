#include <filesystem>

#include "gtest/gtest.h"
#include "test/test_utility/include/test_helper.h"

using ::testing::Test;
using namespace graphflow::testing;

class EndToEndReadLists2BytesPerEdgeTest : public DBTest {
public:
    void initGraph() {
        conn->query(createNodeCmdPrefix + "person (ID INT64, PRIMARY KEY (ID))");
        conn->query("COPY person FROM \"dataset/read-list-tests/2-bytes-per-edge/vPerson.csv\"");
        conn->query(
            createRelCmdPrefix + "knows (FROM person TO person, int64Prop INT64, MANY_MANY)");
        conn->query("COPY knows FROM \"dataset/read-list-tests/2-bytes-per-edge/eKnows.csv\"");
    }
};

class EndToEndReadLists4BytesPerEdgeTest : public DBTest {
public:
    void initGraph() {
        conn->query(createNodeCmdPrefix + "person (ID INT64, PRIMARY KEY (ID))");
        conn->query("COPY person FROM \"dataset/read-list-tests/4-bytes-per-edge/vPerson.csv\"");
        conn->query(createRelCmdPrefix + "knows (FROM person TO person, MANY_MANY)");
        conn->query("COPY knows FROM \"dataset/read-list-tests/4-bytes-per-edge/eKnows.csv\"");
    }
};

class EndToEndReadListsSubQueryTest : public DBTest {
public:
    void initGraph() {
        conn->query(createNodeCmdPrefix + "person (ID INT64, PRIMARY KEY (ID))");
        conn->query("COPY person FROM "
                    "\"dataset/read-list-tests/large-list-sub-query-tests/vPerson.csv\"");
        conn->query(createRelCmdPrefix + "knows (FROM person TO person, MANY_MANY)");
        conn->query(
            "COPY knows FROM \"dataset/read-list-tests/large-list-sub-query-tests/eKnows.csv\"");
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
