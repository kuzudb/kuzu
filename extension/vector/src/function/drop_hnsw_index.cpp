#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "function/hnsw_index_functions.h"
#include "function/table/bind_data.h"
#include "index/hnsw_index_utils.h"
#include "processor/execution_context.h"

using namespace kuzu::function;

namespace kuzu {
namespace vector_extension {

struct DropHNSWIndexBindData final : TableFuncBindData {
    catalog::NodeTableCatalogEntry* tableEntry;
    std::string indexName;

    DropHNSWIndexBindData(catalog::NodeTableCatalogEntry* tableEntry, std::string indexName)
        : TableFuncBindData{0}, tableEntry{tableEntry}, indexName{std::move(indexName)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DropHNSWIndexBindData>(tableEntry, indexName);
    }
};

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    const auto tableName = input->getLiteralVal<std::string>(0);
    const auto indexName = input->getLiteralVal<std::string>(1);
    const auto tableEntry = HNSWIndexUtils::bindNodeTable(*context, tableName, indexName,
        HNSWIndexUtils::IndexOperation::DROP);
    return std::make_unique<DropHNSWIndexBindData>(tableEntry, indexName);
}

static common::offset_t internalTableFunc(const TableFuncInput& input, TableFuncOutput&) {
    const auto& context = *input.context->clientContext;
    const auto bindData = input.bindData->constPtrCast<DropHNSWIndexBindData>();
    context.getCatalog()->dropIndex(context.getTransaction(), bindData->tableEntry->getTableID(),
        bindData->indexName);
    return 0;
}

static std::string dropHNSWIndexTables(main::ClientContext& context,
    const TableFuncBindData& bindData) {
    const auto dropHNSWIndexBindData = bindData.constPtrCast<DropHNSWIndexBindData>();
    context.setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    std::string query = "";
    const auto requireNewTransaction = !context.getTransactionContext()->hasActiveTransaction();
    if (requireNewTransaction) {
        query += "BEGIN TRANSACTION;";
    }
    auto nodeTableID = dropHNSWIndexBindData->tableEntry->getTableID();
    query += common::stringFormat("CALL _DROP_HNSW_INDEX('{}', '{}');",
        dropHNSWIndexBindData->tableEntry->getName(), dropHNSWIndexBindData->indexName);
    query += common::stringFormat("DROP TABLE {};",
        HNSWIndexUtils::getUpperGraphTableName(nodeTableID, dropHNSWIndexBindData->indexName));
    query += common::stringFormat("DROP TABLE {};",
        HNSWIndexUtils::getLowerGraphTableName(nodeTableID, dropHNSWIndexBindData->indexName));
    if (requireNewTransaction) {
        query += "COMMIT;";
    }
    return query;
}

function_set InternalDropHNSWIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes = {common::LogicalTypeID::STRING, common::LogicalTypeID::STRING};
    auto func = std::make_unique<TableFunction>(name, inputTypes);
    func->tableFunc = internalTableFunc;
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    func->isReadOnly = false;
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set DropVectorIndexFunction::getFunctionSet() {
    function_set functionSet;
    std::vector inputTypes = {common::LogicalTypeID::STRING, common::LogicalTypeID::STRING};
    auto func = std::make_unique<TableFunction>(name, inputTypes);
    func->tableFunc = TableFunction::emptyTableFunc;
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->rewriteFunc = dropHNSWIndexTables;
    func->canParallelFunc = [] { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace vector_extension
} // namespace kuzu
