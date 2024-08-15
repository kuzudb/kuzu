#include "processor/operator/ddl/alter.h"

#include "catalog/catalog.h"
#include "common/enums/alter_type.h"
#include "storage/storage_manager.h"
#include "storage/store/table.h"

using namespace kuzu::binder;

namespace kuzu {
namespace processor {

std::string AlterPrintInfo::toString() const {
    std::string result = "Operation: ";
    switch (alterType) {
    case common::AlterType::RENAME_TABLE: {
        auto renameInfo = info->constPtrCast<BoundExtraRenameTableInfo>();
        result += "Rename Table " + tableName + " to " + renameInfo->newName;
    } break;
    case common::AlterType::ADD_PROPERTY: {
        auto addPropInfo = info->constPtrCast<BoundExtraAddPropertyInfo>();
        result +=
            "Add Property " + addPropInfo->propertyDefinition.getName() + " to Table " + tableName;
    } break;
    case common::AlterType::DROP_PROPERTY: {
        auto dropPropInfo = info->constPtrCast<BoundExtraDropPropertyInfo>();
        result += "Drop Property " + dropPropInfo->propertyName + " from Table " + tableName;
    } break;
    case common::AlterType::RENAME_PROPERTY: {
        auto renamePropInfo = info->constPtrCast<BoundExtraRenamePropertyInfo>();
        result += "Rename Property " + renamePropInfo->oldName + " to " + renamePropInfo->newName +
                  " in Table " + tableName;
    } break;
    case common::AlterType::COMMENT: {
        result += "Comment on Table " + tableName;
    } break;
    default:
        break;
    }
    return result;
}

void Alter::executeDDLInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    auto transaction = context->clientContext->getTx();
    const auto storageManager = context->clientContext->getStorageManager();
    catalog->alterTableEntry(transaction, info);
    if (info.alterType == common::AlterType::ADD_PROPERTY) {
        auto& boundAddPropInfo = info.extraInfo->constCast<BoundExtraAddPropertyInfo>();
        KU_ASSERT(defaultValueEvaluator);
        auto entry = catalog->getTableCatalogEntry(transaction, info.tableName);
        auto& addedProp = entry->getProperty(boundAddPropInfo.propertyDefinition.getName());
        storage::TableAddColumnState state{addedProp, *defaultValueEvaluator};
        storageManager->getTable(entry->getTableID())
            ->addColumn(context->clientContext->getTx(), state);
    } else if (info.alterType == common::AlterType::DROP_PROPERTY) {
        const auto schema = context->clientContext->getCatalog()->getTableCatalogEntry(
            context->clientContext->getTx(), info.tableName);
        storageManager->getTable(schema->getTableID())->dropColumn();
    }
}

std::string Alter::getOutputMsg() {
    if (info.alterType == common::AlterType::COMMENT) {
        return common::stringFormat("Table {} comment updated.", info.tableName);
    }
    return common::stringFormat("Table {} altered.", info.tableName);
}

} // namespace processor
} // namespace kuzu
