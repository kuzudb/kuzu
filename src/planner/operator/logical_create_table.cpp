#include "planner/operator/ddl/logical_create_table.h"

#include "binder/expression/function_expression.h"
#include "planner/operator/factorization/flatten_resolver.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

std::string LogicalCreateTablePrintInfo::toString() const {
    std::string result = "";
    auto* tableInfo = info->ptrCast<binder::BoundExtraCreateTableInfo>();
    switch (tableType) {
    case TableType::NODE: {
        result += "Create Node Table: ";
        result += tableName;
        result += ",Properties: ";
        for (auto& prop : tableInfo->propertyInfos) {
            result += prop.name;
            result += ", ";
        }
        break;
    }
    case TableType::REL: {
        result += "Create Relationship Table: ";
        result += tableName;
        result += ",Multiplicity: ";
        auto* relInfo = tableInfo->ptrCast<binder::BoundExtraCreateRelTableInfo>();
        if (relInfo->srcMultiplicity == RelMultiplicity::ONE) {
            result += "ONE";
        } else {
            result += "MANY";
        }
        result += "_";
        if (relInfo->dstMultiplicity == RelMultiplicity::ONE) {
            result += "ONE";
        } else {
            result += "MANY";
        }
        result += ",Properties: ";
        for (auto& prop : tableInfo->propertyInfos) {
            result += prop.name;
            result += ", ";
        }
        break;
    }
    case TableType::REL_GROUP: {
        result += "Create Relationship Group Table: ";
        result += tableName;
        auto* relGroupInfo = info->ptrCast<binder::BoundExtraCreateRelTableGroupInfo>();
        result += ",Tables: ";
        for (auto& createInfo : relGroupInfo->infos) {
            result += createInfo.tableName;
            result += ", ";
        }
        auto* groupTableInfo =
            relGroupInfo->infos[0].extraInfo->ptrCast<binder::BoundExtraCreateTableInfo>();
        result += "Properties: ";
        for (auto& prop : groupTableInfo->propertyInfos) {
            result += prop.name;
            result += ", ";
        }
        break;
    }
    default:
        break;
    }
    return result;
}

} // namespace planner
} // namespace kuzu
