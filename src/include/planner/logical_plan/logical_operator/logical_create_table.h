#pragma once

#include "catalog/catalog_structs.h"
#include "logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalCreateTable : public LogicalDDL {
public:
    LogicalCreateTable(LogicalOperatorType operatorType, std::string tableName,
        std::vector<catalog::Property> properties,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{operatorType, std::move(tableName), std::move(outputExpression)},
          properties{std::move(properties)} {}

    inline std::vector<catalog::Property> getPropertyNameDataTypes() const { return properties; }

protected:
    std::vector<catalog::Property> properties;
};

} // namespace planner
} // namespace kuzu
