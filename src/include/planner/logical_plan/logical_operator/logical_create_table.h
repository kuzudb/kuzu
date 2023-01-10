#pragma once

#include "logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalCreateTable : public LogicalDDL {
public:
    LogicalCreateTable(LogicalOperatorType operatorType, string tableName,
        vector<PropertyNameDataType> propertyNameDataTypes)
        : LogicalDDL{operatorType, std::move(tableName)}, propertyNameDataTypes{
                                                              std::move(propertyNameDataTypes)} {}

    inline vector<PropertyNameDataType> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }

protected:
    vector<PropertyNameDataType> propertyNameDataTypes;
};

} // namespace planner
} // namespace kuzu
