#include "processor/operator/ddl/alter.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "common/enums/alter_type.h"
#include "storage/storage_manager.h"
#include "storage/store/table.h"

using namespace kuzu::binder;

namespace kuzu {
namespace processor {

using skip_alter_on_conflict = std::function<bool()>;

static bool skipAlter(common::ConflictAction action,
    const skip_alter_on_conflict& skipAlterOnConflict) {
    switch (action) {
    case common::ConflictAction::ON_CONFLICT_THROW:
        return false;
    case common::ConflictAction::ON_CONFLICT_DO_NOTHING:
        return skipAlterOnConflict();
    default:
        KU_UNREACHABLE;
    }
}

void Alter::alterRelGroupChildren(const ExecutionContext* context) const {
    auto catalog = context->clientContext->getCatalog();
    auto transaction = context->clientContext->getTransaction();
    const auto* relGroupEntry = catalog->getRelGroupEntry(transaction, info.tableName);
    for (auto tableID : relGroupEntry->getRelTableIDs()) {
        const auto* tableEntry = catalog->getTableCatalogEntry(transaction, tableID);
        BoundAlterInfo tableAlterInfo = info.copy();
        tableAlterInfo.tableName = tableEntry->getName();
        if (tableAlterInfo.alterType == common::AlterType::RENAME_TABLE) {
            KU_ASSERT(tableAlterInfo.tableName.starts_with(relGroupEntry->getName()));
            // table name is in format {rel_group_name}{suffix}
            // rename to {new_rel_group_name}{suffix}
            auto& renameInfo = tableAlterInfo.extraInfo->cast<BoundExtraRenameTableInfo>();
            auto tableNameSuffix = tableEntry->getName().substr(relGroupEntry->getName().size());
            renameInfo.newName.append(tableNameSuffix);
        }
        alterTable(context, tableAlterInfo);
    }
}

void Alter::executeDDLInternal(ExecutionContext* context) {
    auto catalog = context->clientContext->getCatalog();
    auto transaction = context->clientContext->getTransaction();
    if (catalog->containsTable(transaction, info.tableName)) {
        // entry is a table
        alterTable(context, info);
    } else {
        // if the entry isn't a table it should be a rel group
        alterRelGroupChildren(context);
        catalog->alterRelGroupEntry(transaction, info);
    }
}

using on_conflict_throw_action = std::function<void()>;

static void validateProperty(common::ConflictAction action,
    const on_conflict_throw_action& throwAction) {
    switch (action) {
    case common::ConflictAction::ON_CONFLICT_THROW: {
        throwAction();
    } break;
    case common::ConflictAction::ON_CONFLICT_DO_NOTHING:
        break;
    default:
        KU_UNREACHABLE;
    }
}

static void validatePropertyExist(common::ConflictAction action,
    catalog::TableCatalogEntry* tableEntry, const std::string& propertyName) {
    validateProperty(action, [&tableEntry, &propertyName]() {
        if (!tableEntry->containsProperty(propertyName)) {
            throw common::RuntimeException(
                tableEntry->getName() + " table does not have property " + propertyName + ".");
        }
    });
}

static void validatePropertyNotExist(common::ConflictAction action,
    catalog::TableCatalogEntry* tableEntry, const std::string& propertyName) {
    validateProperty(action, [&tableEntry, &propertyName] {
        if (tableEntry->containsProperty(propertyName)) {
            throw common::RuntimeException(
                tableEntry->getName() + " table already has property " + propertyName + ".");
        }
    });
}

static bool checkAddPropertyConflicts(catalog::TableCatalogEntry* tableEntry,
    const BoundAlterInfo& info) {
    const auto* extraInfo = info.extraInfo->constPtrCast<BoundExtraAddPropertyInfo>();
    validatePropertyNotExist(info.onConflict, tableEntry, extraInfo->propertyDefinition.getName());

    // Eventually, we want to support non-constant default on rel tables, but it is non-trivial
    // due
    // to FWD/BWD storage
    if (tableEntry->getType() == catalog::CatalogEntryType::REL_TABLE_ENTRY &&
        extraInfo->boundDefault->expressionType != common::ExpressionType::LITERAL) {
        throw common::RuntimeException(
            "Cannot set a non-constant default value when adding columns on REL tables.");
    }

    return skipAlter(info.onConflict, [&]() {
        return tableEntry->containsProperty(
            info.extraInfo->constCast<BoundExtraAddPropertyInfo>().propertyDefinition.getName());
    });
}

static bool checkDropPropertyConflicts(catalog::TableCatalogEntry* tableEntry,
    const BoundAlterInfo& info) {
    const auto* extraInfo = info.extraInfo->constPtrCast<BoundExtraDropPropertyInfo>();
    validatePropertyExist(info.onConflict, tableEntry, extraInfo->propertyName);

    if (tableEntry->getTableType() == common::TableType::NODE &&
        tableEntry->constCast<catalog::NodeTableCatalogEntry>().getPrimaryKeyName() ==
            extraInfo->propertyName) {
        throw common::RuntimeException("Cannot drop primary key of a node table.");
    }

    return skipAlter(info.onConflict, [&]() {
        return !tableEntry->containsProperty(
            info.extraInfo->constCast<BoundExtraDropPropertyInfo>().propertyName);
    });
}

static bool checkRenamePropertyConflicts(catalog::TableCatalogEntry* tableEntry,
    const BoundAlterInfo& info) {
    const auto* extraInfo = info.extraInfo->constPtrCast<BoundExtraRenamePropertyInfo>();
    validatePropertyExist(common::ConflictAction::ON_CONFLICT_THROW, tableEntry,
        extraInfo->oldName);
    validatePropertyNotExist(common::ConflictAction::ON_CONFLICT_THROW, tableEntry,
        extraInfo->newName);
    return false;
}

static bool checkConflicts(catalog::TableCatalogEntry* tableEntry, const BoundAlterInfo& info) {
    switch (info.alterType) {
    case common::AlterType::ADD_PROPERTY:
        return checkAddPropertyConflicts(tableEntry, info);
    case common::AlterType::DROP_PROPERTY:
        return checkDropPropertyConflicts(tableEntry, info);
    case common::AlterType::RENAME_PROPERTY:
        return checkRenamePropertyConflicts(tableEntry, info);
    default:
        return false;
    }
}

void Alter::alterTable(const ExecutionContext* context, const BoundAlterInfo& alterInfo) const {
    auto catalog = context->clientContext->getCatalog();
    auto transaction = context->clientContext->getTransaction();

    auto entry = catalog->getTableCatalogEntry(transaction, alterInfo.tableName);
    if (checkConflicts(entry, alterInfo)) {
        return;
    }
    const auto storageManager = context->clientContext->getStorageManager();
    catalog->alterTableEntry(transaction, alterInfo);
    if (info.alterType == common::AlterType::ADD_PROPERTY) {
        auto& boundAddPropInfo = info.extraInfo->constCast<BoundExtraAddPropertyInfo>();
        KU_ASSERT(defaultValueEvaluator);
        auto* alteredEntry = catalog->getTableCatalogEntry(transaction, alterInfo.tableName);
        auto& addedProp = alteredEntry->getProperty(boundAddPropInfo.propertyDefinition.getName());
        storage::TableAddColumnState state{addedProp, *defaultValueEvaluator};
        storageManager->getTable(alteredEntry->getTableID())->addColumn(transaction, state);
    } else if (info.alterType == common::AlterType::DROP_PROPERTY) {
        auto* alteredEntry = catalog->getTableCatalogEntry(transaction, alterInfo.tableName);
        storageManager->getTable(alteredEntry->getTableID())->dropColumn();
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
