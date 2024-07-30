#pragma once

#include "binder/ddl/bound_alter_info.h"
#include "logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalAlter : public LogicalDDL {
public:
    LogicalAlter(binder::BoundAlterInfo info, std::string tableName,
        std::shared_ptr<binder::Expression> outputExpression,
        std::unique_ptr<OPPrintInfo> printInfo)
        : LogicalDDL{LogicalOperatorType::ALTER, std::move(tableName), std::move(outputExpression),
              std::move(printInfo)},
          info{std::move(info)} {}

    inline const binder::BoundAlterInfo* getInfo() const { return &info; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAlter>(info.copy(), tableName, outputExpression,
            printInfo->copy());
    }

private:
    binder::BoundAlterInfo info;
};

} // namespace planner
} // namespace kuzu
