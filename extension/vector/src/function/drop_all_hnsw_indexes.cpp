#include "catalog/catalog.h"
#include "catalog/catalog_entry/index_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "catalog/hnsw_index_catalog_entry.h"
#include "function/hnsw_index_functions.h"
#include "function/table/bind_data.h"
#include "index/hnsw_index_utils.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "storage/storage_manager.h"

using namespace kuzu::function;

namespace kuzu {
namespace vector_extension {

struct DropAllHNSWIndexesBindData final : TableFuncBindData {
    catalog::NodeTableCatalogEntry* tableEntry;
    std::vector<std::string> indexNames;

    DropAllHNSWIndexesBindData(catalog::NodeTableCatalogEntry* tableEntry, std::vector<std::string> indexNames)
        : TableFuncBindData{0}, tableEntry{tableEntry}, indexNames{std::move(indexNames)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DropAllHNSWIndexesBindData>(tableEntry, indexNames);
    }
};

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    const auto tableName = input->getLiteralVal<std::string>(0);
    const auto tableEntry = HNSWIndexUtils::bindTable(*context, tableName);
    const auto nodeTableEntry = tableEntry->ptrCast<catalog::NodeTableCatalogEntry>();
    const auto tableID = tableEntry->getTableID();
    std::vector<std::string> vectorIndexes;
    auto indexEntries =
        catalog::Catalog::Get(*context)
            ->getIndexEntries(context->getTransaction(), tableID);
    for (auto indexEntry : indexEntries) {
        if (indexEntry->getIndexType() == HNSWIndexCatalogEntry::TYPE_NAME) {
            vectorIndexes.push_back(indexEntry->getIndexName());
        }
    }
    return std::make_unique<DropAllHNSWIndexesBindData>(nodeTableEntry, vectorIndexes);
}

static std::string dropAllHNSWIndexesTables(main::ClientContext& context,
    const TableFuncBindData& bindData) {
    const auto dropAllHNSWIndexesBindData = bindData.constPtrCast<DropAllHNSWIndexesBindData>();
    context.setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    std::string query = "";
    for (const auto& indexName : dropAllHNSWIndexesBindData->indexNames) {
        const auto requireNewTransaction = !context.getTransactionContext()->hasActiveTransaction();
        if (requireNewTransaction) {
            query += "BEGIN TRANSACTION;";
        }
        auto nodeTableID = dropAllHNSWIndexesBindData->tableEntry->getTableID();
        query += common::stringFormat("CALL _DROP_HNSW_INDEX('{}', '{}');",
            dropAllHNSWIndexesBindData->tableEntry->getName(), indexName);
        query += common::stringFormat("DROP TABLE {};",
            HNSWIndexUtils::getUpperGraphTableName(nodeTableID, indexName));
        query += common::stringFormat("DROP TABLE {};",
            HNSWIndexUtils::getLowerGraphTableName(nodeTableID, indexName));
        if (requireNewTransaction) {
            query += "COMMIT;";
        }
    }
    return query;
}

function_set DropAllVectorIndexesFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes = {common::LogicalTypeID::STRING};
    auto func = std::make_unique<TableFunction>(name, inputTypes);
    func->tableFunc = TableFunction::emptyTableFunc;
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->rewriteFunc = dropAllHNSWIndexesTables;
    func->canParallelFunc = [] { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace vector_extension
} // namespace kuzu
