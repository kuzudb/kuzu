#include "gtest/gtest.h"

#include "src/testing/include/test_helper.h"

using namespace graphflow::testing;

TEST(ProcessorTest, NodeQueries) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runTest("test/runner/queries/structural/nodes.test"));
}

TEST(ProcessorTest, PathQueries) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runTest("test/runner/queries/structural/paths.test"));
}

TEST(ProcessorTest, StarQueries) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runTest("test/runner/queries/structural/stars.test"));
}
