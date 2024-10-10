#pragma once

#include "binder/ddl/bound_alter_info.h"
#include "logical_ddl.h"

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

class LogicalAlter final : public LogicalDDL {
public:
    LogicalAlter(binder::BoundAlterInfo info, std::string tableName,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::ALTER, std::move(tableName), std::move(outputExpression)},
          info{std::move(info)} {}

    const binder::BoundAlterInfo* getInfo() const { return &info; }

    std::unique_ptr<OPPrintInfo> getPrintInfo() const override {
        return std::make_unique<LogicalAlterPrintInfo>(info.copy());
    }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalAlter>(info.copy(), tableName, outputExpression);
    }

private:
    binder::BoundAlterInfo info;
};

} // namespace planner
} // namespace kuzu
