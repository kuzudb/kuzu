#pragma once

#include "src/common/include/file_ser_deser_helper.h"
#include "src/common/include/types.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace planner {

enum LogicalOperatorType {
    SCAN,
    EXTEND,
    FILTER,
    NODE_PROPERTY_READER,
    REL_PROPERTY_READER,
    HASH_JOIN,
};

class LogicalOperator {
public:
    LogicalOperator() = default;

    LogicalOperator(shared_ptr<LogicalOperator> prevOperator) {
        setPrevOperator(move(prevOperator));
    }

    virtual ~LogicalOperator() = default;

    virtual LogicalOperatorType getLogicalOperatorType() const = 0;

    void setPrevOperator(shared_ptr<LogicalOperator> prevOperator) {
        this->prevOperator = prevOperator;
    }

public:
    shared_ptr<LogicalOperator> prevOperator;
};

} // namespace planner
} // namespace graphflow
