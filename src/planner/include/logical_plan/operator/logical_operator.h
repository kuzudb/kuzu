#pragma once

#include "src/common/include/file_ser_deser_helper.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace planner {

enum LogicalOperatorType {
    SCAN,
    NODE_PROPERTY_READER,
    REL_PROPERTY_READER,
    EXTEND,
};

class LogicalOperator {
public:
    LogicalOperator() = default;

    LogicalOperator(unique_ptr<LogicalOperator> prevOperator) {
        setPrevOperator(move(prevOperator));
    }

    virtual ~LogicalOperator() = default;

    virtual LogicalOperatorType getLogicalOperatorType() = 0;

    void setPrevOperator(unique_ptr<LogicalOperator> prevOperator) {
        this->prevOperator.reset(prevOperator.release());
    }

public:
    unique_ptr<LogicalOperator> prevOperator;
};

} // namespace planner
} // namespace graphflow
