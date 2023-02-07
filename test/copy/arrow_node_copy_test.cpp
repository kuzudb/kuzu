#include "graph_test/graph_test.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class ArrowNodeCopyTest : public DBTest {
    void SetUp() override {
        BaseGraphTest::SetUp();
        createDBAndConn();
    }

    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-test/node/csv/");
    }
};

TEST_F(ArrowNodeCopyTest, ArrowNodeCopyCSVTest) {
    initGraphFromPath(TestHelper::appendKuzuRootPath("dataset/copy-test/node/csv/"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/copy/copy_node.test"));
}

TEST_F(ArrowNodeCopyTest, ArrowNodeCopyArrowTest) {
    initGraphFromPath(TestHelper::appendKuzuRootPath("dataset/copy-test/node/arrow/"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/copy/copy_node.test"));
}

TEST_F(ArrowNodeCopyTest, ArrowNodeCopyParquetTest) {
    initGraphFromPath(TestHelper::appendKuzuRootPath("dataset/copy-test/node/parquet/"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/copy/copy_node.test"));
}
