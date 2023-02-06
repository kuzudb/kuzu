#pragma once

#include "logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalCreateTable : public LogicalDDL {
public:
    LogicalCreateTable(LogicalOperatorType operatorType, std::string tableName,
        std::vector<catalog::PropertyNameDataType> propertyNameDataTypes,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{operatorType, std::move(tableName), std::move(outputExpression)},
          propertyNameDataTypes{std::move(propertyNameDataTypes)} {}

    inline std::vector<catalog::PropertyNameDataType> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }

protected:
    std::vector<catalog::PropertyNameDataType> propertyNameDataTypes;
};

} // namespace planner
} // namespace kuzu
