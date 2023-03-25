#include "graph_test/graph_test.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class CopyNodeFromCSVTest : public DBTest {
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-test/node/csv/");
    }
};

TEST_F(CopyNodeFromCSVTest, CopyNodeTest) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/copy/copy_node.test"));
}

class CopyNodeFromParquetTest : public DBTest {
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-test/node/parquet/");
    }
};

TEST_F(CopyNodeFromParquetTest, CopyNodeTest) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/copy/copy_node.test"));
}

class CopyNodeWrongPathTest : public BaseGraphTest {
public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        createDBAndConn();
    }

    std::string getInputDir() override { throw NotImplementedException("getInputDir()"); }
};

TEST_F(CopyNodeWrongPathTest, WrongPathTest) {
    conn->query("CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))");
    auto result = conn->query("COPY User FROM 'wrong_path.csv'");
    ASSERT_FALSE(result->isSuccess());
    result = conn->query("COPY User FROM 'wrong_path.parquet'");
    ASSERT_FALSE(result->isSuccess());
}
