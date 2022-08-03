#pragma once

#include "base_logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalDDL : public LogicalOperator {

public:
    LogicalDDL(string labelName, vector<PropertyNameDataType> propertyNameDataTypes)
        : LogicalOperator{}, labelName{move(labelName)}, propertyNameDataTypes{
                                                             move(propertyNameDataTypes)} {}

    inline string getExpressionsForPrinting() const override { return labelName; }

    inline string getLabelName() const { return labelName; }

    inline vector<PropertyNameDataType> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }

protected:
    string labelName;
    vector<PropertyNameDataType> propertyNameDataTypes;
};

} // namespace planner
} // namespace graphflow
