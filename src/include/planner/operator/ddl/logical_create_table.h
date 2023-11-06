#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "planner/operator/ddl/logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalCreateTable : public LogicalDDL {
public:
    LogicalCreateTable(std::string tableName, std::unique_ptr<binder::BoundCreateTableInfo> info,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::CREATE_TABLE, std::move(tableName),
              std::move(outputExpression)},
          info{std::move(info)} {}

    inline binder::BoundCreateTableInfo* getInfo() const { return info.get(); }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalCreateTable>(tableName, info->copy(), outputExpression);
    }

private:
    std::unique_ptr<binder::BoundCreateTableInfo> info;
};

} // namespace planner
} // namespace kuzu
