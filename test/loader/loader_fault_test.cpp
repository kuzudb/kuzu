#include "test/test_utility/include/test_helper.h"

#include "src/common/include/exception.h"

using namespace graphflow::testing;
using namespace std;

class LoaderFaultTest : public BaseGraphLoadingTest {
public:
    void SetUp() override{};

    string getInputCSVDir() override { return "dataset/long-string-error/"; }

    void loadGraph() {
        testSuiteSystemConfig.graphInputDir = getInputCSVDir();
        testSuiteSystemConfig.graphOutputDir = TEMP_TEST_DIR;
        TestHelper::loadGraph(testSuiteSystemConfig);
    }
};

TEST_F(LoaderFaultTest, LongStringErrors) {
    EXPECT_THROW(loadGraph(), LoaderException);
}
