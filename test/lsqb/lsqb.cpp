#include "graph_test/graph_test.h"

using ::testing::Test;
using namespace kuzu::testing;

class LSQBTest : public DBTest {
public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        systemConfig->bufferPoolSize =
            kuzu::common::BufferPoolConstants::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING * 16;
        createDBAndConn();
        initGraph();
    }

    std::string getInputDir() override { return TestHelper::appendKuzuRootPath("dataset/sf-0.1/"); }
};

TEST_F(LSQBTest, LSQBTest) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/lsqb/lsqb_queries.test"));
}
