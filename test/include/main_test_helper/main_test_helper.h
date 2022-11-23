#pragma once

#include "test_helper/test_helper.h"

namespace kuzu {
namespace testing {

class ApiTest : public EmptyDBTest {

public:
    void SetUp() override {
        EmptyDBTest::SetUp();
        systemConfig->defaultPageBufferPoolSize = (1ull << 26);
        systemConfig->largePageBufferPoolSize = (1ull << 26);
        createDBAndConn();
        initGraph();
    }

    string getInputCSVDir() override { return TestHelper::appendKuzuRootPath("dataset/tinysnb/"); }

    static void assertMatchPersonCountStar(Connection* conn) {
        auto result = conn->query("MATCH (a:person) RETURN COUNT(*)");
        ASSERT_TRUE(result->hasNext());
        auto tuple = result->getNext();
        ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), 8);
        ASSERT_FALSE(result->hasNext());
    }
};
} // namespace testing
} // namespace kuzu
