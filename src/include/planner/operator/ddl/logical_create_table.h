#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "planner/operator/simple/logical_simple.h"

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

class LogicalCreateTable final : public LogicalSimple {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::CREATE_TABLE;

public:
    LogicalCreateTable(binder::BoundCreateTableInfo info,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalSimple{type_, std::move(outputExpression)}, info{std::move(info)} {}

    std::string getExpressionsForPrinting() const override { return info.tableName; }

    const binder::BoundCreateTableInfo* getInfo() const { return &info; }

    std::unique_ptr<OPPrintInfo> getPrintInfo() const override {
        return std::make_unique<LogicalCreateTablePrintInfo>(info.copy());
    }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalCreateTable>(info.copy(), outputExpression);
    }

private:
    binder::BoundCreateTableInfo info;
};

} // namespace planner
} // namespace kuzu
