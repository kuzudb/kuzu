#include "processor/operator/ddl/create_table.h"

#include "common/string_format.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string CreateTablePrintInfo::toString() const {
    std::string result = "";
    auto* tableInfo = common::ku_dynamic_cast<binder::BoundExtraCreateCatalogEntryInfo*,
        binder::BoundExtraCreateTableInfo*>(info);
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
        auto* relInfo = common::ku_dynamic_cast<binder::BoundExtraCreateTableInfo*,
            binder::BoundExtraCreateRelTableInfo*>(tableInfo);
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
        auto* relGroupInfo = common::ku_dynamic_cast<binder::BoundExtraCreateCatalogEntryInfo*,
            binder::BoundExtraCreateRelTableGroupInfo*>(info);
        result += ",Tables: ";
        for (auto& createInfo : relGroupInfo->infos) {
            result += createInfo.tableName;
            result += ", ";
        }
        auto* groupTableInfo = common::ku_dynamic_cast<binder::BoundExtraCreateCatalogEntryInfo*,
            binder::BoundExtraCreateTableInfo*>(relGroupInfo->infos[0].extraInfo.get());
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
