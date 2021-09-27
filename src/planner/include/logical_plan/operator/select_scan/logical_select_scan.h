#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

class LogicalSelectScan : public LogicalOperator {

public:
    LogicalSelectScan(unordered_set<string> variablesToSelect)
        : variablesToSelect{move(variablesToSelect)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SELECT_SCAN;
    }

    string getExpressionsForPrinting() const override {
        string result;
        for (auto& variable : variablesToSelect) {
            result += variable + ", ";
        }
        return result;
    }

    inline const unordered_set<string>& getVariablesToSelect() const { return variablesToSelect; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSelectScan>(variablesToSelect);
    }

private:
    unordered_set<string> variablesToSelect;
};

} // namespace planner
} // namespace graphflow
