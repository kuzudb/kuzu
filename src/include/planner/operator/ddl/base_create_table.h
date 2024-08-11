#pragma once

#include "binder/ddl/bound_create_table_info.h"
#include "planner/operator/ddl/logical_ddl.h"

namespace kuzu {
    
class BaseCreateTablePrintInfo : planner::OPPrintInfo{
public:
    common::TableType tableType;
    std::string tableName;
    binder::BoundExtraCreateCatalogEntryInfo* info;

    BaseCreateTablePrintInfo(common::TableType tableType, std::string tableName,
        binder::BoundExtraCreateCatalogEntryInfo* info)
        : tableType{std::move(tableType)}, tableName{std::move(tableName)}, info{info} {}

    std::string toString() const;

    BaseCreateTablePrintInfo(const BaseCreateTablePrintInfo& other)
        : tableType{other.tableType}, tableName{other.tableName}, info{other.info} {}
};
} // namespace kuzu 