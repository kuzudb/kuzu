#pragma once

#include "src/common/include/file_ser_deser_helper.h"
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
    LOGICAL_UPDATE_NODE
};

const string LogicalOperatorTypeNames[] = {"LOGICAL_SCAN_NODE_ID", "LOGICAL_EXTEND",
    "LOGICAL_FILTER", "LOGICAL_PROJECTION", "LOGICAL_SCAN_NODE_PROPERTY",
    "LOGICAL_SCAN_REL_PROPERTY", "LOGICAL_HASH_JOIN", "LOGICAL_LOAD_CSV", "LOGICAL_CREATE_NODE",
    "LOGICAL_UPDATE_NODE"};

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
