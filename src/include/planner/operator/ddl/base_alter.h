#pragma once


#include "binder/ddl/bound_alter_info.h"
#include "logical_ddl.h"

namespace kuzu {

class BaseAlterPrintInfo {
public:
    common::AlterType alterType;
    std::string tableName;
    binder::BoundExtraAlterInfo* info;

    BaseAlterPrintInfo(common::AlterType alterType, std::string tableName,
        binder::BoundExtraAlterInfo* info)
        : alterType{std::move(alterType)}, tableName{std::move(tableName)}, info{info} {}

    std::string toString() const;

    BaseAlterPrintInfo(const BaseAlterPrintInfo& other)
        : alterType{other.alterType}, tableName{other.tableName}, info{other.info} {}
};
} // namespace kuzu 