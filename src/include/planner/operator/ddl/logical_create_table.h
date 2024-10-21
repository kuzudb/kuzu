#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "planner/operator/ddl/logical_ddl.h"

namespace kuzu {
namespace planner {

struct LogicalCreateTablePrintInfo final : OPPrintInfo {
    binder::BoundCreateTableInfo info;

    explicit LogicalCreateTablePrintInfo(binder::BoundCreateTableInfo info)
        : info{std::move(info)} {}

    std::string toString() const override { return info.toString(); }

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::make_unique<LogicalCreateTablePrintInfo>(*this);
    }

    LogicalCreateTablePrintInfo(const LogicalCreateTablePrintInfo& other)
        : info{other.info.copy()} {}
};

class LogicalCreateTable final : public LogicalDDL {
public:
    LogicalCreateTable(std::string tableName, binder::BoundCreateTableInfo info,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::CREATE_TABLE, std::move(tableName),
              std::move(outputExpression)},
          info{std::move(info)} {}

    const binder::BoundCreateTableInfo* getInfo() const { return &info; }

    std::unique_ptr<OPPrintInfo> getPrintInfo() const override {
        return std::make_unique<LogicalCreateTablePrintInfo>(info.copy());
    }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalCreateTable>(tableName, info.copy(), outputExpression);
    }

private:
    binder::BoundCreateTableInfo info;
};

} // namespace planner
} // namespace kuzu
