#pragma once

#include <vector>

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDDL : public LogicalOperator {

public:
    LogicalDDL(string tableName, vector<PropertyNameDataType> propertyNameDataTypes)
        : LogicalOperator{}, tableName{move(tableName)}, propertyNameDataTypes{
                                                             move(propertyNameDataTypes)} {}

    inline string getExpressionsForPrinting() const override { return tableName; }

    inline string getTableName() const { return tableName; }

    inline vector<PropertyNameDataType> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }

protected:
    string tableName;
    vector<PropertyNameDataType> propertyNameDataTypes;
};

} // namespace planner
} // namespace kuzu
