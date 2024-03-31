#include "processor/operator/ddl/alter.h"

#include "catalog/catalog.h"
#include "common/enums/alter_type.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace processor {

void Alter::executeDDLInternal(ExecutionContext* context) {
    switch (info.alterType) {
    case common::AlterType::ADD_PROPERTY: {
        context->clientContext->getCatalog()->alterTableSchema(info);
        auto& boundAddPropInfo = common::ku_dynamic_cast<const binder::BoundExtraAlterInfo&,
            const binder::BoundExtraAddPropertyInfo&>(*info.extraInfo);
        KU_ASSERT(defaultValueEvaluator);
        auto schema = context->clientContext->getCatalog()->getTableCatalogEntry(
            context->clientContext->getTx(), info.tableID);
        auto addedPropID = schema->getPropertyID(boundAddPropInfo.propertyName);
        auto addedProp = schema->getProperty(addedPropID);
        defaultValueEvaluator->evaluate(context->clientContext);
        auto storageManager = context->clientContext->getStorageManager();
        storageManager->getTable(info.tableID)
            ->addColumn(context->clientContext->getTx(), *addedProp,
                defaultValueEvaluator->resultVector.get());
    } break;
    case common::AlterType::DROP_PROPERTY: {
        auto& boundDropPropInfo = common::ku_dynamic_cast<const binder::BoundExtraAlterInfo&,
            const binder::BoundExtraDropPropertyInfo&>(*info.extraInfo);
        auto tableEntry = context->clientContext->getCatalog()->getTableCatalogEntry(
            context->clientContext->getTx(), info.tableID);
        auto columnID = tableEntry->getColumnID(boundDropPropInfo.propertyID);
        context->clientContext->getCatalog()->alterTableSchema(info);
        if (tableEntry->getTableType() == common::TableType::NODE) {
            auto nodesStats =
                context->clientContext->getStorageManager()->getNodesStatisticsAndDeletedIDs();
            nodesStats->removeMetadataDAHInfo(info.tableID, columnID);
        } else {
            auto relsStats = context->clientContext->getStorageManager()->getRelsStatistics();
            relsStats->removeMetadataDAHInfo(info.tableID, columnID);
        }
    } break;
    default: {
        context->clientContext->getCatalog()->alterTableSchema(info);
        // DO NOTHING.
    }
    }
}

std::string Alter::getOutputMsg() {
    return common::stringFormat("Table {} altered.", info.tableName);
}

} // namespace processor
} // namespace kuzu
