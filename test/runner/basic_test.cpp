#include "gtest/gtest.h"

#include "src/testing/include/test_helper.h"

using namespace graphflow::testing;

TEST(BasicTest, BasicScan) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runTest("test/runner/basic_scan.test"));
}
