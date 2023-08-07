#pragma once

#include "catalog/table_schema.h"
#include "planner/logical_plan/ddl/logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalCreateTable : public LogicalDDL {
public:
    LogicalCreateTable(LogicalOperatorType operatorType, std::string tableName,
        std::vector<std::unique_ptr<catalog::Property>> properties,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{operatorType, std::move(tableName), std::move(outputExpression)},
          properties{std::move(properties)} {}

    inline std::vector<std::unique_ptr<catalog::Property>> getProperties() const {
        return catalog::Property::copyProperties(properties);
    }

protected:
    std::vector<std::unique_ptr<catalog::Property>> properties;
};

} // namespace planner
} // namespace kuzu
