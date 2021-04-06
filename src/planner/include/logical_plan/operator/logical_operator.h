#pragma once

#include "src/common/include/file_ser_deser_helper.h"
#include "src/common/include/types.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace planner {

enum LogicalOperatorType : uint8_t {
    LOGICAL_SCAN,
    LOGICAL_EXTEND,
    LOGICAL_FILTER,
    LOGICAL_NODE_PROPERTY_READER,
    LOGICAL_REL_PROPERTY_READER,
    LOGICAL_HASH_JOIN,
};

const string LogicalOperatorTypeNames[] = {"LOGICAL_SCAN", "LOGICAL_EXTEND", "LOGICAL_FILTER",
    "LOGICAL_NODE_PROPERTY_READER", "LOGICAL_REL_PROPERTY_READER", "LOGICAL_HASH_JOIN"};

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

    virtual string toString(uint64_t depth = 0) const {
        string result = string(depth * 4, ' ');
        result += LogicalOperatorTypeNames[getLogicalOperatorType()] + "[" +
                  getOperatorInformation() + "]";
        if (prevOperator) {
            result += "\n";
            result += prevOperator->toString(depth);
        }
        return result;
    }

    virtual string getOperatorInformation() const = 0;

public:
    shared_ptr<LogicalOperator> prevOperator;
};

} // namespace planner
} // namespace graphflow
