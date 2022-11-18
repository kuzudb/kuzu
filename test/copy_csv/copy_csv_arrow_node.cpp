#include "test/test_utility/include/test_helper.h"

using namespace std;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class CopyCSVArrowNodeTest : public InMemoryDBTest {
    string getInputCSVDir() override { return "dataset/copy-csv-arrow-node-test/"; }
};

TEST_F(CopyCSVArrowNodeTest, ArrowTest) {}