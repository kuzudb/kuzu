#include "storage/index/index_utils.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "main/client_context.h"

namespace kuzu {
namespace storage {

static void validateIndexExistence(const main::ClientContext& context,
    catalog::TableCatalogEntry* tableEntry, const std::string& indexName,
    IndexOperation indexOperation) {
    switch (indexOperation) {
    case IndexOperation::CREATE: {
        if (context.getCatalog()->containsIndex(context.getTransaction(), tableEntry->getTableID(),
                indexName)) {
            throw common::BinderException{common::stringFormat(
                "Index {} already exists in table {}.", indexName, tableEntry->getName())};
        }
    } break;
    case IndexOperation::DROP:
    case IndexOperation::QUERY: {
        if (!context.getCatalog()->containsIndex(context.getTransaction(), tableEntry->getTableID(),
                indexName)) {
            throw common::BinderException{common::stringFormat(
                "Table {} doesn't have an index with name {}.", tableEntry->getName(), indexName)};
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

static void validateTableExistence(const main::ClientContext& context,
    const std::string& tableName) {
    if (!context.getCatalog()->containsTable(context.getTransaction(), tableName)) {
        throw common::BinderException{common::stringFormat("Table {} does not exist.", tableName)};
    }
}

static void validateNodeTable(const catalog::TableCatalogEntry* tableEntry) {
    if (tableEntry->getTableType() != common::TableType::NODE) {
        throw common::BinderException(common::stringFormat(
            "Table {} is not a node table. Only a node table is supported for this function.",
            tableEntry->getName()));
    }
}

catalog::NodeTableCatalogEntry* IndexUtils::bindTable(const main::ClientContext& context,
    const std::string& tableName, const std::string& indexName, IndexOperation indexOperation) {
    validateTableExistence(context, tableName);
    const auto tableEntry =
        context.getCatalog()->getTableCatalogEntry(context.getTransaction(), tableName);
    validateNodeTable(tableEntry);
    validateIndexExistence(context, tableEntry, indexName, indexOperation);
    return tableEntry->ptrCast<catalog::NodeTableCatalogEntry>();
}

void IndexUtils::validateAutoTransaction(const main::ClientContext& context,
    const std::string& funcName) {
    if (!context.getTransactionContext()->isAutoTransaction()) {
        throw common::BinderException{
            common::stringFormat("{} is only supported in auto transaction mode.", funcName)};
    }
}

} // namespace storage
} // namespace kuzu
