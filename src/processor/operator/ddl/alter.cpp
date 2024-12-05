#include "processor/operator/ddl/alter.h"

#include "catalog/catalog.h"
#include "common/enums/alter_type.h"
#include "storage/storage_manager.h"
#include "storage/store/table.h"

using namespace kuzu::binder;

namespace kuzu {
namespace processor {

using skip_alter_on_conflict = std::function<bool()>;

bool skipAlter(common::ConflictAction action, skip_alter_on_conflict skipAlterOnConflict) {
    switch (action) {
    case common::ConflictAction::ON_CONFLICT_THROW:
        return false;
    case common::ConflictAction::ON_CONFLICT_DO_NOTHING:
        return skipAlterOnConflict();
    default:
        KU_UNREACHABLE;
    }
}

void Alter::executeDDLInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    auto transaction = context->clientContext->getTx();
    switch (info.alterType) {
    case common::AlterType::ADD_PROPERTY: {
        if (skipAlter(info.onConflict, [&]() {
                return catalog->getTableCatalogEntry(transaction, info.tableName)
                    ->containsProperty(info.extraInfo->constCast<BoundExtraAddPropertyInfo>()
                                           .propertyDefinition.getName());
            })) {
            return;
        }
    } break;
    case common::AlterType::DROP_PROPERTY: {
        if (skipAlter(info.onConflict, [&]() {
                return !catalog->getTableCatalogEntry(transaction, info.tableName)
                            ->containsProperty(
                                info.extraInfo->constCast<BoundExtraDropPropertyInfo>()
                                    .propertyName);
            })) {
            return;
        }
    } break;
    default:
        break;
    }
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
