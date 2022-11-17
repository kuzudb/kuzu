#include "test/test_utility/include/test_helper.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;
using namespace graphflow::testing;

class CopyCSVArrowNodeTest : public InMemoryDBTest {
    string getInputCSVDir() override { return "dataset/copy-csv-arrow-node-test/"; }
};

TEST_F(CopyCSVArrowNodeTest, ArrowTest) {}