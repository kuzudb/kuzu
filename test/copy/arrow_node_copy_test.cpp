#include <iostream>

#include "test_helper/test_helper.h"

using namespace std;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class arrowNodeCopyTest : public DBTest {
    void SetUp() override {
        BaseGraphTest::SetUp();
        createDBAndConn();
    }

    string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-test/node/csv/");
    }
};

TEST_F(arrowNodeCopyTest, arrowNodeCopyCSVTest) {
    initGraphFromPath(TestHelper::appendKuzuRootPath("dataset/copy-test/node/csv/"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/copy/copy_node.test"));
}

TEST_F(arrowNodeCopyTest, arrowNodeCopyArrowTest) {
    initGraphFromPath(TestHelper::appendKuzuRootPath("dataset/copy-test/node/arrow/"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/copy/copy_node.test"));
}

TEST_F(arrowNodeCopyTest, arrowNodeCopyParquetTest) {
    initGraphFromPath(TestHelper::appendKuzuRootPath("dataset/copy-test/node/parquet/"));
    runTest(TestHelper::appendKuzuRootPath("test/test_files/copy/copy_node.test"));
}
