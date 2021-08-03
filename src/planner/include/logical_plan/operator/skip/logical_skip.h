#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalSkip : public LogicalOperator {

public:
    LogicalSkip(uint64_t skipNumber, string variableToSelect, vector<string> groupsToSkip,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator(move(prevOperator)), skipNumber{skipNumber},
          variableToSelect{move(variableToSelect)}, groupsToSkip{move(groupsToSkip)} {}

    LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_SKIP; }

    string getExpressionsForPrinting() const override { return to_string(skipNumber); }

    inline uint64_t getSkipNumber() const { return skipNumber; }
    inline string getVariableToSelect() const { return variableToSelect; }
    inline const vector<string>& getGroupsToSkip() const { return groupsToSkip; };

private:
    uint64_t skipNumber;
    string variableToSelect;
    vector<string> groupsToSkip;
};

} // namespace planner
} // namespace graphflow
