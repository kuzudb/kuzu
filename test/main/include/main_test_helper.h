#pragma once

#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class ApiTest : public EmptyDBTest {

public:
    void SetUp() override {
        EmptyDBTest::SetUp();
        systemConfig->defaultPageBufferPoolSize = (1ull << 26);
        systemConfig->largePageBufferPoolSize = (1ull << 26);
        createDBAndConn();
        initGraph();
    }

    string getInputCSVDir() override { return "dataset/tinysnb/"; }

    static void assertMatchPersonCountStar(Connection* conn) {
        auto result = conn->query("MATCH (a:person) RETURN COUNT(*)");
        ASSERT_TRUE(result->hasNext());
        auto tuple = result->getNext();
        ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), 8);
        ASSERT_FALSE(result->hasNext());
    }
};
