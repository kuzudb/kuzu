#pragma once

#include "binder/ddl/bound_alter_info.h"
#include "logical_ddl.h"
#include "planner/operator/ddl/base_alter.h"

namespace kuzu {
namespace planner {

struct LogicalAlterPrintInfo final : OPPrintInfo {
    std::unique_ptr<BaseAlterPrintInfo> base;

    LogicalAlterPrintInfo(std::unique_ptr<BaseAlterPrintInfo> base)
        : base(std::move(base)) {}

    std::string toString() const override {return base->toString();};

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<LogicalAlterPrintInfo>(new LogicalAlterPrintInfo(*this));
    }

private:
    LogicalAlterPrintInfo(const LogicalAlterPrintInfo& other)
        : OPPrintInfo{other}, base(std::make_unique<BaseAlterPrintInfo>(*other.base)){}
};

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
        return std::make_unique<LogicalAlter>(info.copy(), tableName, outputExpression,
            printInfo->copy());
    }

private:
    binder::BoundAlterInfo info;
};

} // namespace planner
} // namespace kuzu
