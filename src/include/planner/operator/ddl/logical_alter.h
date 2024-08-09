#pragma once

#include "binder/ddl/bound_alter_info.h"
#include "logical_ddl.h"

namespace kuzu {
namespace planner {

struct LogicalAlterPrintInfo final : OPPrintInfo {
    common::AlterType alterType;
    std::string tableName;
    binder::BoundExtraAlterInfo* info;

    LogicalAlterPrintInfo(common::AlterType alterType, std::string tableName,
        binder::BoundExtraAlterInfo* info)
        : alterType{std::move(alterType)}, tableName{std::move(tableName)}, info{info} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<LogicalAlterPrintInfo>(new LogicalAlterPrintInfo(*this));
    }

private:
    LogicalAlterPrintInfo(const LogicalAlterPrintInfo& other)
        : OPPrintInfo{other}, alterType{other.alterType}, tableName{other.tableName},
          info{other.info} {}
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
        return make_unique<LogicalAlter>(info.copy(), tableName, outputExpression,
            printInfo->copy());
    }

private:
    binder::BoundAlterInfo info;
};

} // namespace planner
} // namespace kuzu
