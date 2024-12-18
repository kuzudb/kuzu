#include "function/query_fts_index.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/parameter_expression.h"
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
    std::shared_ptr<binder::Expression> query;
    const FTSIndexCatalogEntry& entry;
    QueryFTSConfig config;

    QueryFTSBindData(std::string tableName, common::table_id_t tableID, std::string indexName,
        std::shared_ptr<binder::Expression> query, const FTSIndexCatalogEntry& entry,
        binder::expression_vector columns, QueryFTSConfig config)
        : FTSBindData{std::move(tableName), tableID, std::move(indexName), columns},
          query{std::move(query)}, entry{entry}, config{std::move(config)} {}

    std::string getQuery() const;

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<QueryFTSBindData>(*this);
    }
};

std::string QueryFTSBindData::getQuery() const {
    auto value = Value::createDefaultValue(query->dataType);
    switch (query->expressionType) {
    case ExpressionType::LITERAL: {
        value = query->constCast<binder::LiteralExpression>().getValue();
    } break;
    case ExpressionType::PARAMETER: {
        value = query->constCast<binder::ParameterExpression>().getValue();
    } break;
    default:
        KU_UNREACHABLE;
    }
    return value.getValue<std::string>();
}

struct QueryFTSLocalState : public TableFuncLocalState {
    std::unique_ptr<QueryResult> result = nullptr;
    uint64_t numRowsOutput = 0;
};

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    TableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    // For queryFTS, the table and index name must be given at compile time while the user
    // can give the query at runtime.
    auto indexName = binder::ExpressionUtil::getLiteralValue<std::string>(*input->getParam(1));
    auto& tableEntry = FTSUtils::bindTable(
        binder::ExpressionUtil::getLiteralValue<std::string>(*input->getParam(0)), context,
        indexName, FTSUtils::IndexOperation::QUERY);
    auto query = input->getParam(2);
    FTSUtils::validateIndexExistence(*context, tableEntry.getTableID(), indexName);
    auto ftsCatalogEntry =
        context->getCatalog()->getIndex(context->getTx(), tableEntry.getTableID(), indexName);
    columnTypes.push_back(common::StructType::getNodeType(tableEntry));
    columnNames.push_back("node");
    columnTypes.push_back(LogicalType::DOUBLE());
    columnNames.push_back("score");
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    QueryFTSConfig config{input->optionalParams};
    return std::make_unique<QueryFTSBindData>(tableEntry.getName(), tableEntry.getTableID(),
        std::move(indexName), std::move(query), ftsCatalogEntry->constCast<FTSIndexCatalogEntry>(),
        columns, std::move(config));
}

static std::unique_ptr<QueryResult> runQuery(main::ClientContext* context, std::string query) {
    auto result =
        context->queryInternal(query, "", false /* enumerateAllPlans*/, std::nullopt /* queryID*/);
    if (!result->isSuccess()) {
        throw RuntimeException(result->getErrorMessage());
    }
    return result;
}

static common::offset_t tableFunc(TableFuncInput& data, TableFuncOutput& output) {
    // TODO(Xiyang/Ziyi): Currently we don't have a dedicated planner for queryFTS, so
    //  we need a wrapper call function to CALL the actual GDS function.
    auto localState = data.localState->ptrCast<QueryFTSLocalState>();
    if (localState->result == nullptr) {
        auto bindData = data.bindData->cast<QueryFTSBindData>();
        auto actualQuery = bindData.getQuery();
        auto numDocs = bindData.entry.getNumDocs();
        auto avgDocLen = bindData.entry.getAvgDocLen();
        auto query = common::stringFormat("UNWIND tokenize('{}') AS tk RETURN COUNT(DISTINCT tk);",
            actualQuery);
        auto clientContext = data.context->clientContext;
        auto result = runQuery(clientContext, query);
        auto numTermsInQuery = result->getNext()->getValue(0)->toString();
        // Project graph
        query = stringFormat("CALL create_project_graph('PK', ['{}', '{}'], ['{}'])",
            bindData.getTermsTableName(), bindData.getDocsTableName(),
            bindData.getAppearsInTableName());
        runQuery(clientContext, query);
        // Compute score
        query = common::stringFormat(
            "MATCH (a:`{}`) "
            "WHERE list_contains(get_keys('{}', '{}'), a.term) "
            "CALL QFTS(PK, a, {}, {}, cast({} as UINT64), {}, cast({} as UINT64), {}, '{}') "
            "RETURN _node AS p, score",
            bindData.getTermsTableName(), actualQuery, bindData.entry.getFTSConfig().stemmer,
            bindData.config.k, bindData.config.b, numDocs, avgDocLen, numTermsInQuery,
            bindData.config.isConjunctive ? "true" : "false", bindData.tableName);
        localState->result = runQuery(clientContext, query);
        // Remove project graph
        query = stringFormat("CALL drop_project_graph('PK')");
        runQuery(clientContext, query);
    }
    if (localState->numRowsOutput >= localState->result->getTable()->getNumTuples()) {
        return 0;
    }
    auto resultTable = localState->result->getTable();
    resultTable->scan(output.vectors, localState->numRowsOutput, 1 /* numRowsToScan */);
    localState->numRowsOutput++;
    return output.dataChunk.state->getSelSize();
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
