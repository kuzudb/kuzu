#include "processor/operator/ddl/alter.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/enums/alter_type.h"
#include "common/exception/binder.h"
#include "processor/execution_context.h"
#include "storage/storage_manager.h"
#include "storage/store/table.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

void Alter::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    Simple::initLocalStateInternal(resultSet, context);
    if (defaultValueEvaluator) {
        defaultValueEvaluator->init(*resultSet, context->clientContext);
    }
}

void Alter::executeInternal(ExecutionContext* context) {
    auto clientContext = context->clientContext;
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTransaction();
    if (catalog->containsTable(transaction, info.tableName)) {
        auto entry = catalog->getTableCatalogEntry(transaction, info.tableName);
        if (entry->getType() == CatalogEntryType::REL_TABLE_ENTRY) {
            auto parentGroup =
                entry->constCast<RelTableCatalogEntry>().getParentRelGroup(catalog, transaction);
            if (parentGroup != nullptr) {
                throw RuntimeException(
                    stringFormat("Cannot alter table {} because it is referenced by table {}.",
                        info.tableName, parentGroup->getName()));
            }
        }
        alterTable(clientContext, entry, info);
    } else if (catalog->containsRelGroup(transaction, info.tableName)) {
        auto entry = catalog->getRelGroupEntry(transaction, info.tableName);
        alterRelGroup(clientContext, entry, info);
    } else {
        throw BinderException("Table " + info.tableName + " does not exist.");
    }
}

using on_conflict_throw_action = std::function<void()>;

static void validateProperty(ConflictAction action, const on_conflict_throw_action& throwAction) {
    switch (action) {
    case ConflictAction::ON_CONFLICT_THROW: {
        throwAction();
    } break;
    case ConflictAction::ON_CONFLICT_DO_NOTHING:
        break;
    default:
        KU_UNREACHABLE;
    }
}

static void validatePropertyExist(ConflictAction action, TableCatalogEntry* tableEntry,
    const std::string& propertyName) {
    validateProperty(action, [&tableEntry, &propertyName]() {
        if (!tableEntry->containsProperty(propertyName)) {
            throw RuntimeException(
                tableEntry->getName() + " table does not have property " + propertyName + ".");
        }
    });
}

static void validatePropertyNotExist(ConflictAction action, TableCatalogEntry* tableEntry,
    const std::string& propertyName) {
    validateProperty(action, [&tableEntry, &propertyName] {
        if (tableEntry->containsProperty(propertyName)) {
            throw RuntimeException(
                tableEntry->getName() + " table already has property " + propertyName + ".");
        }
    });
}

using skip_alter_on_conflict = std::function<bool()>;

static bool skipAlter(ConflictAction action, const skip_alter_on_conflict& skipAlterOnConflict) {
    switch (action) {
    case ConflictAction::ON_CONFLICT_THROW:
        return false;
    case ConflictAction::ON_CONFLICT_DO_NOTHING:
        return skipAlterOnConflict();
    default:
        KU_UNREACHABLE;
    }
}

static bool checkAddPropertyConflicts(TableCatalogEntry* tableEntry, const BoundAlterInfo& info) {
    const auto* extraInfo = info.extraInfo->constPtrCast<BoundExtraAddPropertyInfo>();
    validatePropertyNotExist(info.onConflict, tableEntry, extraInfo->propertyDefinition.getName());

    // Eventually, we want to support non-constant default on rel tables, but it is non-trivial
    // due to FWD/BWD storage
    if (tableEntry->getType() == CatalogEntryType::REL_TABLE_ENTRY &&
        extraInfo->boundDefault->expressionType != ExpressionType::LITERAL) {
        throw RuntimeException(
            "Cannot set a non-constant default value when adding columns on REL tables.");
    }

    return skipAlter(info.onConflict, [&]() {
        return tableEntry->containsProperty(
            info.extraInfo->constCast<BoundExtraAddPropertyInfo>().propertyDefinition.getName());
    });
}

static bool checkDropPropertyConflicts(TableCatalogEntry* tableEntry, const BoundAlterInfo& info) {
    const auto* extraInfo = info.extraInfo->constPtrCast<BoundExtraDropPropertyInfo>();
    validatePropertyExist(info.onConflict, tableEntry, extraInfo->propertyName);

    if (tableEntry->getTableType() == TableType::NODE &&
        tableEntry->constCast<NodeTableCatalogEntry>().getPrimaryKeyName() ==
            extraInfo->propertyName) {
        throw RuntimeException("Cannot drop primary key of a node table.");
    }

    return skipAlter(info.onConflict, [&]() {
        return !tableEntry->containsProperty(
            info.extraInfo->constCast<BoundExtraDropPropertyInfo>().propertyName);
    });
}

static bool checkRenamePropertyConflicts(TableCatalogEntry* tableEntry,
    const BoundAlterInfo& info) {
    const auto* extraInfo = info.extraInfo->constPtrCast<BoundExtraRenamePropertyInfo>();
    validatePropertyExist(ConflictAction::ON_CONFLICT_THROW, tableEntry, extraInfo->oldName);
    validatePropertyNotExist(ConflictAction::ON_CONFLICT_THROW, tableEntry, extraInfo->newName);
    return false;
}

static void checkNewTableNameNotExist(const std::string& newName, main::ClientContext* context) {
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    if (catalog->containsRelGroup(transaction, newName) ||
        catalog->containsTable(transaction, newName)) {
        throw BinderException("Table " + newName + " already exists.");
    }
}

static bool checkRenameTableConflicts(const BoundAlterInfo& info, main::ClientContext* context) {
    auto newName = info.extraInfo->constCast<BoundExtraRenameTableInfo>().newName;
    checkNewTableNameNotExist(newName, context);
    return false;
}

// Return we should skip alter.
static bool checkAlterTableConflicts(TableCatalogEntry* tableEntry, const BoundAlterInfo& info,
    main::ClientContext* context) {
    switch (info.alterType) {
    case AlterType::ADD_PROPERTY:
        return checkAddPropertyConflicts(tableEntry, info);
    case AlterType::DROP_PROPERTY:
        return checkDropPropertyConflicts(tableEntry, info);
    case AlterType::RENAME_PROPERTY:
        return checkRenamePropertyConflicts(tableEntry, info);
    case AlterType::RENAME:
        return checkRenameTableConflicts(info, context);
    default:
        return false;
    }
}

void Alter::alterTable(main::ClientContext* clientContext, TableCatalogEntry* entry,
    const BoundAlterInfo& alterInfo) const {
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTransaction();
    if (checkAlterTableConflicts(entry, alterInfo, clientContext)) {
        return;
    }
    const auto storageManager = clientContext->getStorageManager();
    catalog->alterTableEntry(transaction, alterInfo);
    switch (info.alterType) {
    case AlterType::ADD_PROPERTY: {
        auto& boundAddPropInfo = info.extraInfo->constCast<BoundExtraAddPropertyInfo>();
        KU_ASSERT(defaultValueEvaluator);
        auto* alteredEntry = catalog->getTableCatalogEntry(transaction, alterInfo.tableName);
        auto& addedProp = alteredEntry->getProperty(boundAddPropInfo.propertyDefinition.getName());
        storage::TableAddColumnState state{addedProp, *defaultValueEvaluator};
        storageManager->getTable(alteredEntry->getTableID())->addColumn(transaction, state);
    } break;
    case AlterType::DROP_PROPERTY: {
        auto* alteredEntry = catalog->getTableCatalogEntry(transaction, alterInfo.tableName);
        storageManager->getTable(alteredEntry->getTableID())->dropColumn();
    } break;
    default:
        break;
    }
}

static void checkAlterRelGroupConflicts(const BoundAlterInfo& info, main::ClientContext* context) {
    switch (info.alterType) {
    case AlterType::RENAME:
        checkRenameTableConflicts(info, context);
    default:
        break;
    }
}

void Alter::alterRelGroup(main::ClientContext* clientContext, RelGroupCatalogEntry* entry,
    const BoundAlterInfo& alterInfo) const {
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTransaction();
    alterRelGroupChildren(clientContext, entry, alterInfo);
    checkAlterRelGroupConflicts(alterInfo, clientContext);
    catalog->alterRelGroupEntry(transaction, info);
}

void Alter::alterRelGroupChildren(main::ClientContext* clientContext, RelGroupCatalogEntry* entry,
    const binder::BoundAlterInfo& alterInfo) const {
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTransaction();
    for (auto tableID : entry->getRelTableIDs()) {
        auto tableEntry = catalog->getTableCatalogEntry(transaction, tableID);
        auto tableAlterInfo = alterInfo.copy();
        tableAlterInfo.tableName = tableEntry->getName();
        if (tableAlterInfo.alterType == AlterType::RENAME) {
            KU_ASSERT(tableAlterInfo.tableName.starts_with(entry->getName()));
            // table name is in format {rel_group_name}{suffix}
            // rename to {new_rel_group_name}{suffix}
            auto& extraInfo = tableAlterInfo.extraInfo->cast<BoundExtraRenameTableInfo>();
            auto tableNameSuffix = tableEntry->getName().substr(entry->getName().size());
            extraInfo.newName.append(tableNameSuffix);
        }
        alterTable(clientContext, tableEntry, tableAlterInfo);
    }
}

std::string Alter::getOutputMsg() {
    if (info.alterType == AlterType::COMMENT) {
        return stringFormat("Table {} comment updated.", info.tableName);
    }
    return stringFormat("Table {} altered.", info.tableName);
}

} // namespace processor
} // namespace kuzu
