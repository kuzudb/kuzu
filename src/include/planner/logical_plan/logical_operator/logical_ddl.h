#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDDL : public LogicalOperator {
public:
    LogicalDDL(LogicalOperatorType operatorType, string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes)
        : LogicalOperator{operatorType}, tableName{std::move(tableName)},
          propertyNameDataTypes{std::move(propertyNameDataTypes)} {}

    inline string getExpressionsForPrinting() const override { return tableName; }

    inline void computeSchema() override { schema = make_unique<Schema>(); }

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
