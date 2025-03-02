#pragma once

#include "binder/ddl/bound_alter_info.h"
#include "planner/operator/simple/logical_simple.h"

namespace kuzu {
namespace planner {

struct LogicalAlterPrintInfo final : OPPrintInfo {
    binder::BoundAlterInfo info;

    explicit LogicalAlterPrintInfo(binder::BoundAlterInfo info) : info{std::move(info)} {}

    std::string toString() const override { return info.toString(); }

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<LogicalAlterPrintInfo>(new LogicalAlterPrintInfo(*this));
    }

    LogicalAlterPrintInfo(const LogicalAlterPrintInfo& other) : info{other.info.copy()} {}
};

class LogicalAlter final : public LogicalSimple {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::ALTER;

public:
    LogicalAlter(binder::BoundAlterInfo info, std::shared_ptr<binder::Expression> outputExpression)
        : LogicalSimple{type_, std::move(outputExpression)}, info{std::move(info)} {}

    std::string getExpressionsForPrinting() const override { return info.tableName; }

    const binder::BoundAlterInfo* getInfo() const { return &info; }

    std::unique_ptr<OPPrintInfo> getPrintInfo() const override {
        return std::make_unique<LogicalAlterPrintInfo>(info.copy());
    }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalAlter>(info.copy(), outputExpression);
    }

private:
    binder::BoundAlterInfo info;
};

} // namespace planner
} // namespace kuzu
