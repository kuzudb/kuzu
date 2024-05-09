#include "processor/operator/ddl/alter.h"

#include "catalog/catalog.h"
#include "common/enums/alter_type.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace processor {

void Alter::executeDDLInternal(ExecutionContext* context) {
    switch (info.alterType) {
    case common::AlterType::ADD_PROPERTY: {
        context->clientContext->getCatalog()->alterTableSchema(context->clientContext->getTx(),
            info);
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
    default: {
        context->clientContext->getCatalog()->alterTableSchema(context->clientContext->getTx(),
            info);
    }
    }
}

std::string Alter::getOutputMsg() {
    return common::stringFormat("Table {} altered.", info.tableName);
}

} // namespace processor
} // namespace kuzu
