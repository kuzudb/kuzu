#include "test/test_utility/include/test_helper.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::testing;

class TinySnbCopyCSVDOSStyleNewlineTest : public InMemoryDBTest {
    string getInputCSVDir() override { return "dataset/copy-csv-dos-style-newline/"; }
};

TEST_F(TinySnbCopyCSVDOSStyleNewlineTest, copyCSVDOSStyleNewline) {
    auto result = conn->query("MATCH (p:person) return p.ID");
    auto groundTruth = vector<string>{"0", "1", "2", "3", "4"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
}
