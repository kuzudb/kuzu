#include "function/fts_index_utils.h"

#include "binder/binder.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "main/client_context.h"

namespace kuzu {
namespace fts_extension {

void FTSIndexUtils::validateIndexExistence(const main::ClientContext& context,
    const catalog::TableCatalogEntry* tableEntry, const std::string& indexName,
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

catalog::NodeTableCatalogEntry* FTSIndexUtils::bindNodeTable(const main::ClientContext& context,
    const std::string& tableName, const std::string& indexName, IndexOperation indexOperation) {
    binder::Binder::validateTableExistence(context, tableName);
    const auto tableEntry =
        context.getCatalog()->getTableCatalogEntry(context.getTransaction(), tableName);
    binder::Binder::validateNodeTableType(tableEntry);
    validateIndexExistence(context, tableEntry, indexName, indexOperation);
    return tableEntry->ptrCast<catalog::NodeTableCatalogEntry>();
}

void FTSIndexUtils::validateAutoTransaction(const main::ClientContext& context,
    const std::string& funcName) {
    if (!context.getTransactionContext()->isAutoTransaction()) {
        throw common::BinderException{
            common::stringFormat("{} is only supported in auto transaction mode.", funcName)};
    }
}

} // namespace fts_extension
} // namespace kuzu
