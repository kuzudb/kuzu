#include "graph_test/graph_test.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class TinySnbCopyDOSStyleNewlineTest : public DBTest {
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/copy-dos-style-newline/");
    }
};

TEST_F(TinySnbCopyDOSStyleNewlineTest, copyDOSStyleNewline) {
    auto result = conn->query("MATCH (p:person) return p.ID");
    auto groundTruth = std::vector<std::string>{"0", "1", "2", "3", "4"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
}
