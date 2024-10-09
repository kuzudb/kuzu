#include "function/query_fts_index.h"

#include "binder/expression/expression_util.h"
#include "catalog/catalog.h"
#include "catalog/fts_index_catalog_entry.h"
#include "common/exception/binder.h"
#include "fts_extension.h"
#include "function/fts_bind_data.h"
#include "function/fts_utils.h"
#include "function/table/bind_input.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::function;

struct QueryFTSBindData final : public FTSBindData {
    std::string query;
    const FTSIndexCatalogEntry& entry;

    QueryFTSBindData(std::string tableName, common::table_id_t tableID, std::string indexName,
        std::string query, const FTSIndexCatalogEntry& entry, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames)
        : FTSBindData{std::move(tableName), tableID, std::move(indexName), std::move(returnTypes),
              std::move(returnColumnNames)},
          query{std::move(query)}, entry{entry} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<QueryFTSBindData>(*this);
    }
};

struct QueryFTSLocalState : public TableFuncLocalState {
    std::unique_ptr<QueryResult> result = nullptr;
    uint64_t numRowsOutput = 0;
};

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    ScanTableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    auto indexName = input->inputs[1].toString();
    auto& tableEntry =
        FTSUtils::bindTable(input->inputs[0], context, indexName, FTSUtils::IndexOperation::QUERY);
    auto query = input->inputs[2].toString();
    FTSUtils::validateIndexExistence(*context, tableEntry.getTableID(), indexName);
    auto ftsCatalogEntry =
        context->getCatalog()->getIndex(context->getTx(), tableEntry.getTableID(), indexName);
    columnTypes.push_back(common::StructType::getNodeType(tableEntry));
    columnNames.push_back("node");
    columnTypes.push_back(LogicalType::DOUBLE());
    columnNames.push_back("score");
    return std::make_unique<QueryFTSBindData>(tableEntry.getName(), tableEntry.getTableID(),
        std::move(indexName), std::move(query), ftsCatalogEntry->constCast<FTSIndexCatalogEntry>(),
        std::move(columnTypes), std::move(columnNames));
}

static common::offset_t tableFunc(TableFuncInput& data, TableFuncOutput& output) {
    // TODO(Xiyang/Ziyi): Currently we don't have a dedicated planner for queryFTS, so
    //  we need a wrapper call function to CALL the actual GDS function.
    auto localState = data.localState->ptrCast<QueryFTSLocalState>();
    if (localState->result == nullptr) {
        auto bindData = data.bindData->constPtrCast<QueryFTSBindData>();
        auto numDocs = bindData->entry.getNumDocs();
        auto avgDocLen = bindData->entry.getAvgDocLen();
        auto query = common::stringFormat("PROJECT GRAPH PK (`{}`, `{}`, `{}`) "
                                          "UNWIND tokenize('{}') AS tk "
                                          "WITH collect(stem(tk, 'porter')) AS tokens "
                                          "MATCH (a:`{}`) "
                                          "WHERE list_contains(tokens, a.term) "
                                          "CALL FTS(PK, a, 1.2, 0.75, cast({} as UINT64), {}) "
                                          "MATCH (p:`{}`) "
                                          "WHERE _node.docID = offset(id(p)) "
                                          "RETURN p, score",
            bindData->getTermsTableName(), bindData->getDocsTableName(),
            bindData->getAppearsInTableName(), bindData->query, bindData->getTermsTableName(),
            numDocs, avgDocLen, bindData->tableName);
        localState->result = data.context->clientContext->queryInternal(query, "", false,
            std::nullopt /* queryID */);
    }
    if (localState->numRowsOutput >= localState->result->getNumTuples()) {
        return 0;
    }
    auto resultTable = localState->result->getTable();
    resultTable->scan(output.vectors, localState->numRowsOutput, 1 /* numRowsToScan */);
    localState->numRowsOutput++;
    return 1;
}

std::unique_ptr<TableFuncLocalState> initLocalState(
    kuzu::function::TableFunctionInitInput& /*input*/, kuzu::function::TableFuncSharedState*,
    storage::MemoryManager*) {
    return std::make_unique<QueryFTSLocalState>();
}

function_set QueryFTSFunction::getFunctionSet() {
    function_set functionSet;
    auto func =
        std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState, initLocalState,
            std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
                LogicalTypeID::STRING});
    func->canParallelFunc = []() { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace fts_extension
} // namespace kuzu
