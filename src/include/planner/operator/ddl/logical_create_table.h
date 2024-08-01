#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "planner/operator/ddl/logical_ddl.h"

namespace kuzu {
namespace planner {

struct LogicalCreateTablePrintInfo final : OPPrintInfo {
    common::TableType tableType;
    std::string tableName;
    binder::BoundExtraCreateCatalogEntryInfo* info;

    LogicalCreateTablePrintInfo(common::TableType tableType, std::string tableName,
        binder::BoundExtraCreateCatalogEntryInfo* info)
        : tableType{std::move(tableType)}, tableName{std::move(tableName)}, info{info} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<LogicalCreateTablePrintInfo>(new LogicalCreateTablePrintInfo(*this));
    }

private:
    LogicalCreateTablePrintInfo(const LogicalCreateTablePrintInfo& other)
        : OPPrintInfo{other}, tableType{other.tableType}, tableName{other.tableName},
          info{other.info} {}
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
