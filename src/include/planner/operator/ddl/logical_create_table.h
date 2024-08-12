#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "processor/operator/ddl/ddl.h"
#include "planner/operator/ddl/base_create_table.h"

namespace kuzu {
namespace planner {

struct LogicalCreateTablePrintInfo final : OPPrintInfo{
    std::unique_ptr<BaseCreateTablePrintInfo> base;

    LogicalCreateTablePrintInfo(std::unique_ptr<BaseCreateTablePrintInfo> base)
        : base(std::move(base)) {}

    std::string toString() const override {return base->toString();};

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<LogicalCreateTablePrintInfo>(new LogicalCreateTablePrintInfo(*this));
    }
    
private:
    LogicalCreateTablePrintInfo(const LogicalCreateTablePrintInfo& other)
        : OPPrintInfo{other}, base(std::make_unique<BaseCreateTablePrintInfo>(*other.base)) {}
};

class LogicalCreateTable : public LogicalDDL {
public:
    LogicalCreateTable(std::string tableName, binder::BoundCreateTableInfo info,
        std::shared_ptr<binder::Expression> outputExpression,
        std::unique_ptr<OPPrintInfo> printInfo)
        : LogicalDDL{LogicalOperatorType::CREATE_TABLE, std::move(tableName),
              std::move(outputExpression), std::move(printInfo)},
          info{std::move(info)} {}

    inline const binder::BoundCreateTableInfo* getInfo() const { return &info; }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalCreateTable>(tableName, info.copy(), outputExpression,
            printInfo->copy());
    }

private:
    binder::BoundCreateTableInfo info;
};

} // namespace planner
} // namespace kuzu
