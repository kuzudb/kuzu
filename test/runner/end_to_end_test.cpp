#include "gtest/gtest.h"

#include "src/testing/include/test_helper.h"

using namespace graphflow::testing;

TEST(ProcessorTest, StructuralNodeQueries) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runTest("test/runner/queries/structural/nodes.test"));
}

TEST(ProcessorTest, StructuralPathQueries) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runTest("test/runner/queries/structural/paths.test"));
}

TEST(ProcessorTest, StructuralStarQueries) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runTest("test/runner/queries/structural/stars.test"));
}

TEST(ProcessorTest, FilteredNodesQueries) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runTest("test/runner/queries/filtered/nodes.test"));
}

TEST(ProcessorTest, FilteredPathQueries) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runTest("test/runner/queries/filtered/paths.test"));
}
