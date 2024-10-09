#include "function/query_fts_index.h"

#include "binder/expression/expression_util.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "fts_extension.h"
#include "function/table/bind_input.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::function;

struct QueryFTSBindData final : public CallTableFuncBindData {
    std::string tableName;
    std::string indexName;
    std::string query;

    QueryFTSBindData(std::string tableName, std::string indexName, std::string query,
        std::vector<LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          tableName{std::move(tableName)}, indexName{std::move(indexName)},
          query{std::move(query)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<QueryFTSBindData>(*this);
    }
};

struct QueryFTSLocalState : public TableFuncLocalState {
    std::unique_ptr<QueryResult> result = nullptr;
    uint64_t numRowsOutput = 0;
};

static LogicalType bindNodeType(std::string tableName, ClientContext* context) {
    if (!context->getCatalog()->containsTable(context->getTx(), tableName)) {
        throw BinderException{common::stringFormat("Table {} does not exist.", tableName)};
    }
    std::vector<StructField> nodeFields;
    nodeFields.emplace_back(InternalKeyword::ID, LogicalType::INTERNAL_ID());
    nodeFields.emplace_back(InternalKeyword::LABEL, LogicalType::STRING());
    auto tableEntry = context->getCatalog()->getTableCatalogEntry(context->getTx(), tableName);
    for (auto& property : tableEntry->getProperties()) {
        nodeFields.emplace_back(property.getName(), property.getType().copy());
    }
    return LogicalType::NODE(std::make_unique<StructTypeInfo>(std::move(nodeFields)));
}

static void validateTableType(catalog::TableCatalogEntry* tableCatalogEntry) {
    if (tableCatalogEntry->getTableType() != common::TableType::NODE) {
        throw common::BinderException{common::stringFormat(
            "Table: {} is not a node table. Can only run full text search on node tables.",
            tableCatalogEntry->getName())};
    }
}

static void validateIndexExistence(catalog::NodeTableCatalogEntry* nodeTableCatalogEntry,
    std::string indexName) {
    if (!nodeTableCatalogEntry->containsIndex(indexName)) {
        throw common::BinderException{
            common::stringFormat("Index: {} has not been built on table: {}.",
                nodeTableCatalogEntry->getName(), indexName)};
    }
}

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    ScanTableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    auto tableName = input->inputs[0].toString();
    auto tableEntry = context->getCatalog()->getTableCatalogEntry(context->getTx(), tableName);
    validateTableType(tableEntry);
    auto indexName = input->inputs[1].toString();
    auto query = input->inputs[2].toString();
    validateIndexExistence(tableEntry->ptrCast<catalog::NodeTableCatalogEntry>(), indexName);
    columnTypes.push_back(bindNodeType(tableName, context));
    columnNames.push_back("node");
    columnTypes.push_back(LogicalType::DOUBLE());
    columnNames.push_back("score");
    return std::make_unique<QueryFTSBindData>(std::move(tableName), std::move(indexName),
        std::move(query), std::move(columnTypes), std::move(columnNames), 1);
}

static common::offset_t tableFunc(TableFuncInput& data, TableFuncOutput& output) {
    // TODO(Xiyang/Ziyi): Currently we don't have a dedicated planner for queryFTS, so
    //  we need a wrapper call function to CALL the actual GDS function.
    auto localState = data.localState->ptrCast<QueryFTSLocalState>();
    if (localState->result == nullptr) {
        auto bindData = data.bindData->constPtrCast<QueryFTSBindData>();
        auto tablePrefix = bindData->tableName + "_" + bindData->indexName;
        auto query =
            common::stringFormat("MATCH (a:{}_stats) RETURN a.num_docs, a.avg_dl", tablePrefix);
        auto result = data.context->runQuery(query);
        auto tuple = result->getNext();
        auto numDocs = tuple->getValue(0)->getValue<uint64_t>();
        auto avgDL = tuple->getValue(1)->getValue<double_t>();
        query = common::stringFormat("PROJECT GRAPH PK ({}_dict, {}_docs, {}_terms) "
                                     "UNWIND {}_tokenize('{}') AS tk "
                                     "WITH collect(stem(tk, 'porter')) AS tokens "
                                     "MATCH (a:{}_dict) "
                                     "WHERE list_contains(tokens, a.term) "
                                     "CALL FTS(PK, a, 1.2, 0.75, cast({} as UINT64), {}) "
                                     "MATCH (p:{}) "
                                     "WHERE _node.docID = offset(id(p)) "
                                     "RETURN p, score",
            tablePrefix, tablePrefix, tablePrefix, tablePrefix, bindData->query, tablePrefix,
            numDocs, avgDL, bindData->tableName);
        localState->result = data.context->runQuery(query);
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
