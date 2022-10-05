#include "test/test_utility/include/test_helper.h"

using ::testing::Test;
using namespace graphflow::testing;

class TinySnbUnwindTest : public DBTest {
public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }
};

TEST_F(TinySnbUnwindTest, MatchExecute) {
    runTest("test/test_files/tinySNB/unwind/unwind_expr.test");
}
