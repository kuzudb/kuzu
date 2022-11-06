#include "test/test_utility/include/test_helper.h"

using ::testing::Test;
using namespace graphflow::testing;

class DemoDBTest : public DBTest {
public:
    string getInputCSVDir() override { return "dataset/demo-db/"; }
};

TEST_F(DemoDBTest, DemoDBTest) {
    runTest("test/test_files/demo_db/demo_db.test");
}
