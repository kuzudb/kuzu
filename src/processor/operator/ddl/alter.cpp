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
    case common::AlterType::RENAME_TABLE:
    case common::AlterType::RENAME_PROPERTY:
    case common::AlterType::SET_COMMENT: {
        context->clientContext->getCatalog()->alterTableSchema(info);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

std::string Alter::getOutputMsg() {
    if (info.alterType != common::AlterType::SET_COMMENT) {
        return common::stringFormat("Table {} altered.", info.tableName);
    } else {
        auto setCommentInfo = common::ku_dynamic_cast<const binder::BoundExtraAlterInfo&,
            const binder::BoundExtraSetCommentInfo&>(*info.extraInfo);
        switch (setCommentInfo.commentType) {
        case common::CommentType::TABLE_ENTRY:
            return common::stringFormat("Table {} comment updated.", info.tableName);
        case common::CommentType::SCALAR_MACRO_ENTRY:
            return common::stringFormat("Macro {} comment updated.", info.tableName);
        }
    }
}

} // namespace processor
} // namespace kuzu
