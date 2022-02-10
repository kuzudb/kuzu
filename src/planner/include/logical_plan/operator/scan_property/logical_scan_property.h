#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalScanProperty : public LogicalOperator {

public:
    LogicalScanProperty(
        string propertyName, uint32_t propertyKey, shared_ptr<LogicalOperator> child)
        : LogicalScanProperty{
              vector<string>{move(propertyName)}, vector<uint32_t>{propertyKey}, move(child)} {}

    LogicalScanProperty(vector<string> propertyNames, vector<uint32_t> propertyKeys,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, propertyNames{move(propertyNames)}, propertyKeys{move(
                                                                                propertyKeys)} {}

    inline vector<string> getPropertyNames() const { return propertyNames; }
    inline vector<uint32_t> getPropertyKeys() const { return propertyKeys; }

    LogicalOperatorType getLogicalOperatorType() const override = 0;

    string getExpressionsForPrinting() const override {
        auto result = string();
        for (auto& propertyName : propertyNames) {
            result += ", " + propertyName;
        }
        return result;
    }

    unique_ptr<LogicalOperator> copy() override = 0;

protected:
    vector<string> propertyNames;
    vector<uint32_t> propertyKeys;
};

} // namespace planner
} // namespace graphflow
