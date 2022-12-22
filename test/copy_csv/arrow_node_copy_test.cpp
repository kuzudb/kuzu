#include "test_helper/test_helper.h"
#include <iostream>

using namespace std;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class arrowNodeCopyCsvTest : public DBTest {
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-test/node/csv/");
    }
};

TEST_F(arrowNodeCopyCsvTest, arrowNodeCopyCsvTest) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/copy/copy_node.test"));
}

class arrowNodeCopyArrowTest : public DBTest {
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-test/node/arrow/");
    }
};

TEST_F(arrowNodeCopyArrowTest, arrowNodeCopyArrowTest) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/copy/copy_node.test"));
}

class arrowNodeCopyParquetTest : public DBTest {
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-test/node/parquet/");
    }
};

TEST_F(arrowNodeCopyParquetTest, arrowNodeCopyParquetTest) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/copy/copy_node.test"));
}
