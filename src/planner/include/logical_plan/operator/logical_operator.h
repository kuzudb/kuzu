#pragma once

#include <memory>

#include "src/common/include/types.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace planner {

enum LogicalOperatorType : uint8_t {
    LOGICAL_SCAN_NODE_ID,
    LOGICAL_EXTEND,
    LOGICAL_FILTER,
    LOGICAL_PROJECTION,
    LOGICAL_SCAN_NODE_PROPERTY,
    LOGICAL_SCAN_REL_PROPERTY,
    LOGICAL_HASH_JOIN,
    LOGICAL_LOAD_CSV,
    LOGICAL_CREATE_NODE,
    LOGICAL_UPDATE_NODE,
    LOGICAL_DELETE_NODE
};

const string LogicalOperatorTypeNames[] = {"LOGICAL_SCAN_NODE_ID", "LOGICAL_EXTEND",
    "LOGICAL_FILTER", "LOGICAL_PROJECTION", "LOGICAL_SCAN_NODE_PROPERTY",
    "LOGICAL_SCAN_REL_PROPERTY", "LOGICAL_HASH_JOIN", "LOGICAL_LOAD_CSV", "LOGICAL_CREATE_NODE",
    "LOGICAL_UPDATE_NODE", "LOGICAL_DELETE_NODE"};

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
                  getExpressionsForPrinting() + "]";
        if (prevOperator) {
            result += "\n";
            result += prevOperator->toString(depth);
        }
        return result;
    }

    virtual string getExpressionsForPrinting() const = 0;

public:
    shared_ptr<LogicalOperator> prevOperator;
};

} // namespace planner
} // namespace graphflow
