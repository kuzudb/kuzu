#pragma once

#include "graph_test/base_graph_test.h"

namespace kuzu {
namespace testing {

class ApiTest : public BaseGraphTest {
public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        createDBAndConn();
        initGraph();
    }

    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }

    static void assertMatchPersonCountStar(main::Connection* conn) {
        auto result = conn->query("MATCH (a:person) RETURN COUNT(*)");
        ASSERT_TRUE(result->hasNext());
        auto tuple = result->getNext();
        ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 8);
        ASSERT_FALSE(result->hasNext());
    }

    static void executeLongRunningQuery(main::Connection* conn) {
        auto result = conn->query(
            "UNWIND RANGE(1,100000) AS x UNWIND RANGE(1, 100000) AS y RETURN COUNT(x + y);");
        ASSERT_FALSE(result->isSuccess());
        ASSERT_EQ(result->getErrorMessage(), "Interrupted.");
    }
};

} // namespace testing
} // namespace kuzu
