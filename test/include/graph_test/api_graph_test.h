#pragma once

#include <cstring>

#include "graph_test/base_graph_test.h"
#include "main/kuzu.h"

using ::testing::Test;

namespace kuzu {
namespace testing {

// This class starts database without initializing graph.
class APIEmptyDBTest : public BaseGraphTest {
    std::string getInputDir() override { throw common::NotImplementedException("getInputDir()"); }
};

// This class starts database in on-disk mode.
class APIDBTest : public BaseGraphTest {
public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        createDBAndConn();
        initGraph();
    }

    inline void CheckConn(std::string connName) {
        if (connMap[connName] == nullptr) {
            connMap[connName] = std::make_unique<main::Connection>(database.get());
        }
    }
};
} // namespace testing
} // namespace kuzu
