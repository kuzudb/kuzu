#include "function/fts_utils.h"

#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "common/types/value/nested.h"

namespace kuzu {
namespace fts_extension {

catalog::NodeTableCatalogEntry& FTSUtils::bindTable(const std::string& tableName,
    main::ClientContext* context, std::string indexName, IndexOperation operation) {
    if (!context->getCatalog()->containsTable(context->getTx(), tableName)) {
        throw common::BinderException{common::stringFormat("Table {} does not exist.", tableName)};
    }
    auto tableEntry = context->getCatalog()->getTableCatalogEntry(context->getTx(), tableName);
    if (tableEntry->getTableType() != common::TableType::NODE) {
        switch (operation) {
        case IndexOperation::CREATE:
            throw common::BinderException{
                common::stringFormat("Table: {} is not a node table. Can only build full text "
                                     "search index on node tables.",
                    tableEntry->getName())};
        case IndexOperation::QUERY:
        case IndexOperation::DROP:
            throw common::BinderException{common::stringFormat(
                "Table: {} doesn't have an index with name: {}. Only node tables can "
                "have full text search indexes.",
                tableEntry->getName(), indexName)};
        }
    }
    auto nodeTableEntry = tableEntry->ptrCast<catalog::NodeTableCatalogEntry>();
    return *nodeTableEntry;
}

void FTSUtils::validateIndexExistence(const main::ClientContext& context,
    common::table_id_t tableID, std::string indexName) {
    if (!context.getCatalog()->containsIndex(context.getTx(), tableID, indexName)) {
        auto tableName = context.getCatalog()->getTableName(context.getTx(), tableID);
        throw common::BinderException{common::stringFormat(
            "Table: {} doesn't have an index with name: {}.", tableName, indexName)};
    }
}

void FTSUtils::validateAutoTrx(const main::ClientContext& context, const std::string& funcName) {
    if (!context.getTransactionContext()->isAutoTransaction()) {
        throw common::BinderException{
            common::stringFormat("{} is only supported in auto transaction mode.", funcName)};
    }
}

} // namespace fts_extension
} // namespace kuzu
