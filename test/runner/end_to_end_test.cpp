#include "gtest/gtest.h"

#include "src/testing/include/test_helper.h"

using namespace graphflow::testing;

TEST(FrontEndTest, SyntaxExceptionQueries) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runExceptionTest("test/runner/queries/malformed/syntax.test"));
}

TEST(FrontEndTest, ExceptionQueries) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runExceptionTest("test/runner/queries/malformed/binder.test"));
}

TEST(FrontEndTest, ExpressionBinderException) {
    TestHelper testHelper;
    ASSERT_TRUE(
        testHelper.runExceptionTest("test/runner/queries/malformed/expression_binder.test"));
}

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

TEST(ProcessorTest, FilteredStarQueries) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runTest("test/runner/queries/filtered/stars.test"));
}

TEST(ProcessorTest, StrOperations) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runTest("test/runner/queries/filtered/str_operations.test"));
}

TEST(ProcessorTest, UnstructuredOperations) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runTest("test/runner/queries/filtered/unstructured_properties.test"));
}

TEST(ProcessorTest, IDComparisonQueries) {
    TestHelper testHelper;
    ASSERT_TRUE(testHelper.runTest("test/runner/queries/filtered/id_comparison.test"));
}
