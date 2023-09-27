#pragma once

#include "binder/ddl/bound_alter_info.h"
#include "logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalAlter : public LogicalDDL {
public:
    LogicalAlter(std::unique_ptr<binder::BoundAlterInfo> info, std::string tableName,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::ALTER, std::move(tableName), std::move(outputExpression)},
          info{std::move(info)} {}

    inline binder::BoundAlterInfo* getInfo() const { return info.get(); }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAlter>(info->copy(), tableName, outputExpression);
    }

private:
    std::unique_ptr<binder::BoundAlterInfo> info;
};

} // namespace planner
} // namespace kuzu
