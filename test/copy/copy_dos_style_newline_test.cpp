#include "graph_test/graph_test.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class TinySnbCopyDOSStyleNewlineTest : public InMemoryDBTest {
    string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-dos-style-newline/");
    }
};

TEST_F(TinySnbCopyDOSStyleNewlineTest, copyDOSStyleNewline) {
    auto result = conn->query("MATCH (p:person) return p.ID");
    auto groundTruth = vector<string>{"0", "1", "2", "3", "4"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
}
