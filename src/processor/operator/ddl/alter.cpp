#include "processor/operator/ddl/alter.h"

#include "catalog/catalog.h"
#include "common/enums/alter_type.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace processor {

std::string AlterPrintInfo::toString() const {
    std::string result = "Operation: ";
    switch (alterType) {
    case common::AlterType::RENAME_TABLE: {
        auto renameInfo = common::ku_dynamic_cast<binder::BoundExtraAlterInfo*,
            binder::BoundExtraRenameTableInfo*>(info);
        result += "Rename Table " + tableName + " to " + renameInfo->newName;
        break;
    }
    case common::AlterType::ADD_PROPERTY: {
        auto addPropInfo = common::ku_dynamic_cast<binder::BoundExtraAlterInfo*,
            binder::BoundExtraAddPropertyInfo*>(info);
        result += "Add Property " + addPropInfo->propertyName + " to Table " + tableName;
        break;
    }
    case common::AlterType::DROP_PROPERTY: {
        auto dropPropInfo = common::ku_dynamic_cast<binder::BoundExtraAlterInfo*,
            binder::BoundExtraDropPropertyInfo*>(info);
        result += "Drop Property " + dropPropInfo->propertyName + " from Table " + tableName;
        break;
    }
    case common::AlterType::RENAME_PROPERTY: {
        auto renamePropInfo = common::ku_dynamic_cast<binder::BoundExtraAlterInfo*,
            binder::BoundExtraRenamePropertyInfo*>(info);
        result += "Rename Property " + renamePropInfo->oldName + " to " + renamePropInfo->newName +
                  " in Table " + tableName;
        break;
    }
    case common::AlterType::COMMENT: {
        result += "Comment on Table " + tableName;
        break;
    }
    default:
        break;
    }
    return result;
}

void Alter::executeDDLInternal(ExecutionContext* context) {
    context->clientContext->getCatalog()->alterTableSchema(context->clientContext->getTx(), info);
    if (info.alterType == common::AlterType::ADD_PROPERTY) {
        auto& boundAddPropInfo = common::ku_dynamic_cast<const binder::BoundExtraAlterInfo&,
            const binder::BoundExtraAddPropertyInfo&>(*info.extraInfo);
        KU_ASSERT(defaultValueEvaluator);
        auto schema = context->clientContext->getCatalog()->getTableCatalogEntry(
            context->clientContext->getTx(), info.tableID);
        auto addedPropID = schema->getPropertyID(boundAddPropInfo.propertyName);
        auto addedProp = schema->getProperty(addedPropID);
        auto storageManager = context->clientContext->getStorageManager();
        storageManager->getTable(info.tableID)
            ->addColumn(context->clientContext->getTx(), *addedProp, *defaultValueEvaluator);
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
