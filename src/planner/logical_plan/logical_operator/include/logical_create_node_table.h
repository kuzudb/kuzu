#pragma once

#include "base_logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalCreateNodeTable : public LogicalOperator {

public:
    LogicalCreateNodeTable(
        string labelName, string primaryKey, vector<PropertyNameDataType> propertyNameDataTypes)
        : LogicalOperator{}, labelName{move(labelName)}, primaryKey{move(primaryKey)},
          propertyNameDataTypes{move(propertyNameDataTypes)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LOGICAL_CREATE_NODE_TABLE;
    }

    inline string getExpressionsForPrinting() const override { return labelName; }

    inline string getLabelName() const { return labelName; }

    inline string getPrimaryKey() const { return primaryKey; }

    inline vector<PropertyNameDataType> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreateNodeTable>(labelName, primaryKey, propertyNameDataTypes);
    }

private:
    string labelName;
    string primaryKey;
    vector<PropertyNameDataType> propertyNameDataTypes;
};

} // namespace planner
} // namespace graphflow
