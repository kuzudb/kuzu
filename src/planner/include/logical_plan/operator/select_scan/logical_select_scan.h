#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

class LogicalSelectScan : public LogicalOperator {

public:
    LogicalSelectScan(vector<string> variablesToSelect, unique_ptr<Schema> outerSchema)
        : variablesToSelect{move(variablesToSelect)}, outerSchema{move(outerSchema)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SELECT_SCAN;
    }

    string getExpressionsForPrinting() const override {
        auto result = variablesToSelect[0];
        for (auto i = 1u; i < variablesToSelect.size(); ++i) {
            result += ", " + variablesToSelect[i];
        }
        return result;
    }

    inline const vector<string>& getVariablesToSelect() const { return variablesToSelect; }
    inline Schema* getOuterSchema() const { return outerSchema.get(); }

private:
    vector<string> variablesToSelect;
    unique_ptr<Schema> outerSchema;
};

} // namespace planner
} // namespace graphflow
