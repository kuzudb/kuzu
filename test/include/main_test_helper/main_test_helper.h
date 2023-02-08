#pragma once

#include "graph_test/graph_test.h"
#include "json.hpp"

namespace kuzu {
namespace testing {

class ApiTest : public BaseGraphTest {

public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        systemConfig->defaultPageBufferPoolSize = (1ull << 26);
        systemConfig->largePageBufferPoolSize = (1ull << 26);
        createDBAndConn();
        initGraph();
    }

    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }

    static void assertMatchPersonCountStar(Connection* conn) {
        auto result = conn->query("MATCH (a:person) RETURN COUNT(*)");
        ASSERT_TRUE(result->hasNext());
        auto tuple = result->getNext();
        ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 8);
        ASSERT_FALSE(result->hasNext());
    }
};
} // namespace testing
} // namespace kuzu
