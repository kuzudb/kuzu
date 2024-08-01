#include "processor/operator/ddl/create_table.h"

#include "common/string_format.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string CreateTablePrintInfo::toString() const {
    std::string result = "";
    auto* tableInfo = info->ptrCast<binder::BoundExtraCreateTableInfo>();
    switch (tableType) {
    case TableType::NODE: {
        result += "Create Node Table: ";
        result += tableName;
        result += ",Properties: ";
        for (auto& definition : tableInfo->propertyDefinitions) {
            result += definition.getName();
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
        for (auto& definition : tableInfo->propertyDefinitions) {
            result += definition.getName();
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
        for (auto& definition : groupTableInfo->propertyDefinitions) {
            result += definition.getName();
            result += ", ";
        }
        break;
    }
    default:
        break;
    }
    return result;
}

void CreateTable::executeDDLInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    switch (info.onConflict) {
    case common::ConflictAction::ON_CONFLICT_DO_NOTHING: {
        if (catalog->containsTable(context->clientContext->getTx(), info.tableName)) {
            return;
        }
    }
    default:
        break;
    }
    auto newTableID = catalog->createTableSchema(context->clientContext->getTx(), info);
    tableCreated = true;
    auto storageManager = context->clientContext->getStorageManager();
    storageManager->createTable(newTableID, catalog, context->clientContext);
}

std::string CreateTable::getOutputMsg() {
    if (tableCreated) {
        return stringFormat("Table {} has been created.", info.tableName);
    }
    return stringFormat("Table {} already exists.", info.tableName);
}

} // namespace processor
} // namespace kuzu
