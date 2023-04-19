/*#include "graph_test/graph_test.h"

using ::testing::Test;
using namespace kuzu::testing;

class SingleSourceShortestPathTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/shortest-path-tests/");
    }
};

TEST_F(SingleSourceShortestPathTest, BFS_SSSP) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/shortest_path/bfs_sssp.test"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/shortest_path/bfs_sssp_large.test"));
}

TEST_F(SingleSourceShortestPathTest, SSSP_ExceptionTests) {
    auto result = conn->query("MATCH p = (a:person)-[:knows*]->(b:person) WHERE a.fName = "
                              "'Alice' RETURN a.fName, b.fName, p.length");
    ASSERT_STREQ("Binder exception: Binding query pattern to path variable is only supported for "
                 "Shortest Path queries.",
        result->getErrorMessage().c_str());

    result = conn->query("MATCH p = (a:person) RETURN a");
    ASSERT_STREQ("Binder exception: Binding path to a single node is not supported.",
        result->getErrorMessage().c_str());

    result = conn->query("MATCH p = (a:person)-[r:knows* SHORTEST]->(b:person) WHERE a.fName = "
                         "'Alice' RETURN a.fName, b.fName, p.length");
    ASSERT_STREQ(
        "Binder exception: Rel variable for Shortest path expression queries are not allowed.",
        result->getErrorMessage().c_str());
}
*/
