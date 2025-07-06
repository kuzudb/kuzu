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
};

} // namespace testing
} // namespace kuzu
