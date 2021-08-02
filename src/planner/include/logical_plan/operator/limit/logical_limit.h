#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalLimit : public LogicalOperator {

public:
    LogicalLimit(uint64_t limitNumber, string variableToSelect, vector<string> groupsToLimit,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, limitNumber{limitNumber},
          variableToSelect{move(variableToSelect)}, groupsToLimit{move(groupsToLimit)} {}

    LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_LIMIT; }

    string getExpressionsForPrinting() const override { return to_string(limitNumber); }

    inline uint64_t getLimitNumber() const { return limitNumber; }
    inline string getVariableToSelect() const { return variableToSelect; }
    inline const vector<string>& getGroupsToLimit() const { return groupsToLimit; };

private:
    uint64_t limitNumber;
    string variableToSelect;
    vector<string> groupsToLimit;
};

} // namespace planner
} // namespace graphflow
