#include "function/create_fts_index.h"

#include "binder/ddl/bound_alter_info.h"
#include "binder/expression/expression_util.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/types/value/nested.h"
#include "fts_extension.h"
#include "function/table/bind_input.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::function;

struct CreateFTSBindData final : public StandaloneTableFuncBindData {
    std::string tableName;
    std::string indexName;
    std::vector<std::string> properties;

    CreateFTSBindData(std::string tableName, std::string indexName,
        std::vector<std::string> properties)
        : StandaloneTableFuncBindData{}, tableName{std::move(tableName)},
          indexName{std::move(indexName)}, properties{std::move(properties)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CreateFTSBindData>(tableName, indexName, properties);
    }
};

static void validateIndexNotExist(catalog::NodeTableCatalogEntry* entry,
    const std::string& indexName) {
    if (entry->containsIndex(indexName)) {
        throw common::BinderException{common::stringFormat(
            "Index with name: {} already exists in table: {}.", indexName, entry->getName())};
    }
}

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    ScanTableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.push_back("");
    columnTypes.push_back(LogicalType::STRING());
    auto tableName = input->inputs[0].toString();
    auto indexName = input->inputs[1].toString();
    std::vector<std::string> properties;
    auto& fieldsToBuildFTS = input->inputs[2];
    for (auto i = 0u; i < fieldsToBuildFTS.getChildrenSize(); i++) {
        properties.push_back(NestedVal::getChildVal(&fieldsToBuildFTS, i)->toString());
    }
    if (!context->getCatalog()->containsTable(context->getTx(), tableName)) {
        throw BinderException{common::stringFormat("Table {} does not exist.", tableName)};
    }
    validateIndexNotExist(context->getCatalog()
                              ->getTableCatalogEntry(context->getTx(), tableName)
                              ->ptrCast<catalog::NodeTableCatalogEntry>(),
        indexName);
    return std::make_unique<CreateFTSBindData>(tableName, indexName, std::move(properties));
}

std::string createFTSIndexQuery(ClientContext& context, const TableFuncBindData& bindData) {
    auto createFTSBindData = bindData.constPtrCast<CreateFTSBindData>();
    auto tableName = createFTSBindData->tableName;
    auto indexName = createFTSBindData->indexName;
    auto properties = createFTSBindData->properties;
    binder::BoundAlterInfo boundAlterInfo{common::AlterType::ADD_INDEX, tableName,
        std::make_unique<binder::BoundExtraIndexInfo>(indexName)};
    // TODO(Ziyi): Copy statement can't be wrapped in manual transaction, so we can't wrap all
    // statements in a single transaction there.
    context.getTransactionContext()->commit();
    context.getTransactionContext()->beginAutoTransaction(false /* readOnly */);
    context.getCatalog()->alterTableEntry(context.getTx(), std::move(boundAlterInfo));
    context.getTransactionContext()->commit();
    context.getTransactionContext()->beginAutoTransaction(true /* readOnly */);
    auto tablePrefix = common::stringFormat("{}_{}", tableName, indexName);
    // Create the tokenize macro.
    std::string query = common::stringFormat(R"(CREATE MACRO {}_tokenize(query) AS
                            string_split(lower(regexp_replace(
                            CAST(query as STRING),
                            '[0-9!@#$%^&*()_+={{}}\\[\\]:;<>,.?~\\\\/\\|\'"`-]+',
                            ' ',
                            'g')), ' ');)",
        tablePrefix);

    // Create the stop words table.
    query += common::stringFormat("CREATE NODE TABLE {}_stopwords (sw STRING, PRIMARY KEY(sw));",
        tablePrefix);
    for (auto i = 0u; i < FTSExtension::NUM_STOP_WORDS; i++) {
        query += common::stringFormat("CREATE (s:{}_stopwords {sw: \"{}\"});", tablePrefix,
            FTSExtension::STOP_WORDS[i]);
    }

    // Create the terms_in_doc table which servers as a temporary table to store the relationship
    // between terms and docs.
    query += common::stringFormat(
        "CREATE NODE TABLE {}_terms_in_doc (ID SERIAL, term string, docID INT64, primary "
        "key(ID));",
        tablePrefix);
    for (auto& property : properties) {
        query += common::stringFormat("COPY {}_terms_in_doc FROM "
                                      "(MATCH (b:{}) "
                                      "WITH {}_tokenize(b.{}) AS tk, OFFSET(ID(b)) AS id "
                                      "UNWIND tk AS t "
                                      "WITH t AS t1, id AS id1 "
                                      "WHERE t1 is NOT NULL AND SIZE(t1) > 0 AND "
                                      "NOT EXISTS {MATCH (s:{}_stopwords {sw: t1})} "
                                      "RETURN STEM(t1, 'porter'), id1);",
            tablePrefix, tableName, tablePrefix, property, tablePrefix);
    }
    // Create the docs table which records the number of words in each document.
    query += common::stringFormat(
        "CREATE NODE TABLE {}_docs (docID INT64, len UINT64, primary key(docID));", tablePrefix);
    query += common::stringFormat("COPY {}_docs FROM "
                                  "(MATCH (t:{}_terms_in_doc) "
                                  "RETURN t.docID, CAST(count(t) AS UINT64) "
                                  "ORDER BY t.docID);",
        tablePrefix, tablePrefix);
    // Create the dic table which records all distinct terms and their document frequency.
    query += common::stringFormat(
        "CREATE NODE TABLE {}_dict (term STRING, df UINT64, PRIMARY KEY(term));", tablePrefix);
    query += common::stringFormat("COPY {}_dict FROM "
                                  "(MATCH (t:{}_terms_in_doc) "
                                  "RETURN t.term, CAST(count(distinct t.docID) AS UINT64));",
        tablePrefix, tablePrefix);
    // Finally, create a terms table that records the documents in which the terms appear, along
    // with the frequency of each term.
    query += common::stringFormat(
        "CREATE REL TABLE {}_terms (FROM {}_dict TO {}_docs, tf UINT64, MANY_MANY);", tablePrefix,
        tablePrefix, tablePrefix);
    query += common::stringFormat("COPY {}_terms FROM ("
                                  "MATCH (b:{}_terms_in_doc) "
                                  "RETURN b.term, b.docID, CAST(count(*) as UINT64));",
        tablePrefix, tablePrefix);
    // Stats table records the number of documents and the average document length.
    query += common::stringFormat(
        "CREATE NODE TABLE {}_stats (ID SERIAL, num_docs UINT64, avg_dl DOUBLE, PRIMARY KEY(ID));",
        tablePrefix);
    query += common::stringFormat("COPY {}_stats FROM (MATCH (d:{}_docs) "
                                  "RETURN CAST(count(d) AS UINT64), "
                                  "CAST(SUM(d.len) AS DOUBLE) / CAST(COUNT(d.len) AS DOUBLE));",
        tablePrefix, tablePrefix);
    return query;
}

static common::offset_t tableFunc(TableFuncInput& /*data*/, TableFuncOutput& /*output*/) {
    KU_UNREACHABLE;
}

function_set CreateFTSFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState,
        initEmptyLocalState,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::LIST});
    func->rewriteFunc = createFTSIndexQuery;
    func->canParallelFunc = []() { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace fts_extension
} // namespace kuzu
