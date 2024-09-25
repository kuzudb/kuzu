#include "function/query_fts_index.h"

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
    std::string query;

    QueryFTSBindData(std::string tableName, std::string query, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          tableName{std::move(tableName)}, query{std::move(query)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<QueryFTSBindData>(tableName, query, LogicalType::copy(columnTypes),
            columnNames, maxOffset);
    }
};

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    ScanTableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    auto tableName = input->inputs[0].toString();
    if (!context->getCatalog()->containsTable(context->getTx(), tableName)) {
        throw BinderException{common::stringFormat("Table {} does not exist.", tableName)};
    }
    auto result = context->runQuery(common::stringFormat("MATCH (d:{}) RETURN d;", tableName));
    columnTypes.push_back(result->getColumnDataTypes()[0].copy());
    columnNames.push_back("node");
    columnTypes.push_back(LogicalType::DOUBLE());
    columnNames.push_back("score");
    auto query = input->inputs[1].toString();
    return std::make_unique<QueryFTSBindData>(tableName, query, std::move(columnTypes),
        std::move(columnNames), result->getNumTuples());
}

static common::offset_t tableFunc(TableFuncInput& data, TableFuncOutput& output) {
    auto bindData = data.bindData->constPtrCast<QueryFTSBindData>();
    auto& dataChunk = output.dataChunk;
    auto sharedState = data.sharedState->ptrCast<CallFuncSharedState>();
    auto outputVector = dataChunk.getValueVector(0);
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        return 0;
    }
    auto tableName = bindData->tableName;
    auto context = data.context;
    auto table = context->getCatalog()
                     ->getTableCatalogEntry(context->getTx(), tableName)
                     ->constPtrCast<catalog::NodeTableCatalogEntry>();
    auto& pk = table->getPrimaryKeyDefinition();
    auto query = common::stringFormat(
        "CREATE NODE TABLE {}_query_terms (ID SERIAL, term STRING, PRIMARY KEY(ID))", tableName);
    context->runQuery(query);
    query = common::stringFormat("COPY {}_query_terms FROM "
                                 "(UNWIND tokenize('{}') AS tk "
                                 "RETURN stem(tk, 'porter'))",
        tableName, bindData->query);
    context->runQuery(query);
    query = common::stringFormat("CREATE NODE TABLE {}_query_terms_in_doc (ID SERIAL, term STRING, "
                                 "docID {},PRIMARY KEY(ID))",
        tableName, pk.getType().toString());
    context->runQuery(query);
    query = common::stringFormat("COPY {}_query_terms_in_doc FROM ("
                                 "MATCH (d1:{}_dict)-[:{}_terms]->(d2:{}_docs) "
                                 "WHERE EXISTS "
                                 "{MATCH (:{}_query_terms {term: d1.term})} "
                                 "RETURN d1.term, d2.docID);",
        tableName, tableName, tableName, tableName, tableName);
    context->runQuery(query);

    query = common::stringFormat("CREATE NODE TABLE {}_terms_tf (ID SERIAL,docID {}, term STRING, "
                                 "tf UINT64, PRIMARY KEY(ID))",
        tableName, pk.getType().toString());
    context->runQuery(query);
    query = common::stringFormat("COPY {}_terms_tf FROM ("
                                 "MATCH (doc:{}_query_terms_in_doc) "
                                 "RETURN doc.docID, doc.term, cast(count(*) as UINT64));",
        tableName, tableName);
    context->runQuery(query);
    query = common::stringFormat("MATCH (s:{}_stats) RETURN s.num_docs, s.avg_dl;", tableName);
    auto result = context->runQuery(query);
    auto tuple = result->getNext();
    auto numDocs = tuple->getValue(0)->getValue<uint64_t>();
    auto avgDL = tuple->getValue(1)->getValue<double>();
    query = common::stringFormat(
        "CREATE NODE TABLE {}_scores (ID SERIAL,docID {}, score DOUBLE, PRIMARY KEY(ID))",
        tableName, pk.getType().toString());
    context->runQuery(query);
    query = common::stringFormat("COPY {}_scores FROM "
                                 "(MATCH (t:{}_terms_tf), (d:{}_docs), (dic:{}_dict) "
                                 "WITH dic.df as df, t.tf as tf, t,d,dic "
                                 "WHERE t.docID = d.docID AND t.term = dic.term "
                                 "RETURN t.docID, sum( "
                                 "log("
                                 "({} - df + 0.5) /  (df + 0.5) + 1) * "
                                 "((tf * ({} + 1) / (tf + {} * (1 - {} + {} * (d.len / {})))))))",
        tableName, tableName, tableName, tableName, numDocs, 1.2 /* default k val */,
        1.2 /* default k val */, 0.75 /* default b val */, 0.75 /* default b val */, avgDL);
    context->runQuery(query);

    query = common::stringFormat("MATCH (doc: {})"
                                 "OPTIONAL MATCH (d: {}_scores) "
                                 "WHERE d.docID = doc.{} "
                                 "RETURN doc, d.score",
        tableName, tableName, pk.getName());
    result = context->runQuery(query);
    auto resultTable = result->getTable();
    std::vector<common::ValueVector*> vectors;
    vectors.push_back(output.dataChunk.getValueVector(0).get());
    vectors.push_back(output.dataChunk.getValueVector(1).get());
    resultTable->scan(vectors, 0 /* startOffset */, resultTable->getTotalNumFlatTuples());
    // Drop helper tables
    query = common::stringFormat("DROP TABLE {}_query_terms", tableName);
    context->runQuery(query);
    query = common::stringFormat("DROP TABLE {}_query_terms_in_doc", tableName);
    context->runQuery(query);
    query = common::stringFormat("DROP TABLE {}_terms_tf", tableName);
    context->runQuery(query);
    query = common::stringFormat("DROP TABLE {}_scores", tableName);
    context->runQuery(query);
    return vectors[0]->state->getSelVectorUnsafe().getSelSize();
}

function_set QueryFTSFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState,
        initEmptyLocalState,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING});
    func->canParallelFunc = []() { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace fts_extension
} // namespace kuzu
