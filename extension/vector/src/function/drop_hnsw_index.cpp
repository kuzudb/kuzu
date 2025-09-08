#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "function/hnsw_index_functions.h"
#include "function/table/bind_data.h"
#include "index/hnsw_index_utils.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "storage/storage_manager.h"
#include "transaction/transaction_context.h"

using namespace kuzu::function;

namespace kuzu {
namespace vector_extension {

struct DropHNSWIndexBindData final : TableFuncBindData {
    catalog::NodeTableCatalogEntry* tableEntry;
    std::string indexName;
    bool skipAfterBind;

    DropHNSWIndexBindData(catalog::NodeTableCatalogEntry* tableEntry, std::string indexName,
        bool skipAfterBind = false)
        : TableFuncBindData{0}, tableEntry{tableEntry}, indexName{std::move(indexName)},
          skipAfterBind{skipAfterBind} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DropHNSWIndexBindData>(tableEntry, indexName, skipAfterBind);
    }
};

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    const auto tableName = input->getLiteralVal<std::string>(0);
    const auto indexName = input->getLiteralVal<std::string>(1);
    auto config = DropHNSWConfig{input->optionalParams};
    const auto nodeTableEntry = HNSWIndexUtils::bindNodeTable(*context, tableName);
    if (!HNSWIndexUtils::validateIndexExistence(*context, nodeTableEntry, indexName,
            HNSWIndexUtils::IndexOperation::DROP, config.conflictAction)) {
        return std::make_unique<DropHNSWIndexBindData>(nullptr, indexName, true);
    }
    return std::make_unique<DropHNSWIndexBindData>(nodeTableEntry, indexName);
}

static common::offset_t internalTableFunc(const TableFuncInput& input, TableFuncOutput&) {
    const auto& context = *input.context->clientContext;
    const auto bindData = input.bindData->constPtrCast<DropHNSWIndexBindData>();
    auto tableID = bindData->tableEntry->getTableID();
    auto transaction = transaction::Transaction::Get(context);
    catalog::Catalog::Get(context)->dropIndex(transaction, tableID, bindData->indexName);
    storage::StorageManager::Get(context)->getTable(tableID)->cast<storage::NodeTable>().dropIndex(
        bindData->indexName);
    return 0;
}

static std::string dropHNSWIndexTables(main::ClientContext& context,
    const TableFuncBindData& bindData) {
    const auto dropHNSWIndexBindData = bindData.constPtrCast<DropHNSWIndexBindData>();
    context.setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    if (dropHNSWIndexBindData->skipAfterBind) {
        return "";
    }
    std::string query = "";
    const auto requireNewTransaction =
        !transaction::TransactionContext::Get(context)->hasActiveTransaction();
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
