#include "function/create_fts_index.h"

#include "binder/expression/expression_util.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/types/value/nested.h"
#include "fts_extension.h"
#include "function/table/bind_input.h"
#include "parser/expression/parsed_literal_expression.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::function;

struct CreateFTSBindData final : public CallTableFuncBindData {

    std::string tableName;
    std::string indexName;
    std::vector<std::string> properties;

    CreateFTSBindData(std::string tableName, std::string indexName,
        std::vector<std::string> properties, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          tableName{std::move(tableName)}, indexName{std::move(indexName)},
          properties{std::move(properties)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CreateFTSBindData>(tableName, indexName, properties,
            LogicalType::copy(columnTypes), columnNames, maxOffset);
    }
};

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
    return std::make_unique<CreateFTSBindData>(tableName, indexName, std::move(properties),
        std::move(columnTypes), std::move(columnNames), 1 /* one row result */);
}

std::string createFTSIndexQuery(ClientContext& context,
    std::vector<parser::ParsedExpression*> parameters) {
    for (auto& parameter : parameters) {
        parser::ParsedExpressionUtils::validateType(*parameter, ExpressionType::LITERAL);
        parser::ParsedExpressionUtils::validateDataType(*parameter, LogicalType::STRING());
    }
    auto tableName = parser::ParsedExpressionUtils::getStringLiteralValue(*parameters[0]);
    auto indexName = parser::ParsedExpressionUtils::getStringLiteralValue(*parameters[1]);
    auto properties = parameters[2]->constCast<parser::ParsedLiteralExpression>().getValue();

    // Create tokenize macro.
    std::string query = "CREATE MACRO tokenize(query) AS "
                        "string_split(regexp_replace(CAST(query as STRING), "
                        "'[0-9!@#$%^&*()_+={}\[\]:;<>,.?~\/\|\\'\"-]+', ' '), ' ');";

    // Create stop words table.
    query += common::stringFormat("CREATE NODE TABLE {}_{}_stopwords (sw STRING, PRIMARY KEY(sw));",
        tableName);
    for (auto i = 0u; i < FTSExtension::NUM_STOP_WORDS; i++) {
        query += common::stringFormat("CREATE (s:{}_{}_stopwords {sw: \"{}\"});", tableName,
            indexName, FTSExtension::STOP_WORDS[i]);
    }

    // Create terms_in_doc table which servers as a temporary table to store the relationship
    // between terms and docs.
    query += "CREATE NODE TABLE {}_{}_terms_in_doc (ID SERIAL, term string, docID INT64, primary "
             "key(ID))";
    for (auto i = 0u; i < properties.getChildrenSize(); i++) {
        auto propertyValue = NestedVal::getChildVal(&properties, i);
        KU_ASSERT(propertyValue->getDataType() == LogicalType::STRING());
        auto property = propertyValue->getValue<std::string>();
        query += common::stringFormat("COPY {}_{}_terms_in_doc FROM "
                                      "(MATCH (b:{}) "
                                      "WITH tokenize(b.{}) AS tk, OFFSET(ID(b)) AS id "
                                      "UNWIND tk AS t "
                                      "WITH t AS t1, id AS id1 "
                                      "WHERE t1 is NOT NULL AND SIZE(t1) > 0 AND NOT EXISTS {MATCH "
                                      "(s:{}_stopwords {sw: t1})} "
                                      "RETURN STEM(t1, 'porter'), id1);",
            tableName, indexName, tableName, property, tableName);
    }
    query += common::stringFormat(
        "CREATE NODE TABLE {}_{}_docs (docID INT64, len UINT64, primary key(offset))", tableName,
        indexName);
    query += common::stringFormat("COPY {}_{}_docs FROM "
                                  "(MATCH (n:{}), (t:{}_{}_terms_in_doc) "
                                  "WHERE OFFSET(ID(n)) = t.offset "
                                  "RETURN OFFSET(ID(n)), CAST(count(t) AS UINT64) "
                                  "ORDER BY OFFSET(ID(n)));",
        tableName, indexName, tableName, tableName, indexName);

    return query;
}

static common::offset_t tableFunc(TableFuncInput& data, TableFuncOutput& output) {
    auto bindData = data.bindData->constPtrCast<CreateFTSBindData>();
    auto& dataChunk = output.dataChunk;
    auto sharedState = data.sharedState->ptrCast<CallFuncSharedState>();
    auto& outputVector = dataChunk.getValueVector(0);
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        return 0;
    }
    auto context = data.context;
    auto catalog = context->getCatalog();
    auto tableName = bindData->tableName;
    std::string query = "CREATE MACRO tokenize(query) AS "
                        "string_split(regexp_replace(CAST(query as STRING), "
                        "'[0-9!@#$%^&*()_+={}\[\]:;<>,.?~\/\|\\'\"-]+', ' '), ' ');";
    context->runQuery(query);
    query = common::stringFormat("CREATE NODE TABLE {}_stopwords (sw STRING, PRIMARY KEY(sw));",
        tableName);
    context->runQuery(query);
    for (auto i = 0u; i < FTSExtension::NUM_STOP_WORDS; i++) {
        context->runQuery(common::stringFormat("CREATE (s:{}_stopwords {sw: \"{}\"})", tableName,
            FTSExtension::STOP_WORDS[i]));
    }

    query = common::stringFormat(
        "CREATE NODE TABLE {}_terms_list (ID SERIAL, term string, offset INT64, primary key(ID))",
        tableName);
    context->runQuery(query);
    for (auto& property : bindData->properties) {
        query = common::stringFormat("COPY {}_terms_list FROM "
                                     "(MATCH (b:{}) "
                                     "WITH tokenize(b.{}) AS tk, OFFSET(ID(b)) AS id "
                                     "UNWIND tk AS t "
                                     "WITH t AS t1, id AS id1 "
                                     "WHERE t1 is NOT NULL AND SIZE(t1) > 0 AND NOT EXISTS {MATCH "
                                     "(s:{}_stopwords {sw: t1})} "
                                     "RETURN STEM(t1, 'porter'), id1);",
            tableName, tableName, property, tableName);
        context->runQuery(query);
    }

    query = common::stringFormat(
        "CREATE NODE TABLE {}_docs (offset INT64, len UINT64, primary key(offset))", tableName);
    context->runQuery(query);
    query = common::stringFormat("COPY {}_docs FROM "
                                 "(MATCH (n:{}), (t:{}_terms_list) "
                                 "WHERE OFFSET(ID(n)) = t.offset "
                                 "RETURN OFFSET(ID(n)), CAST(count(t) AS UINT64) "
                                 "ORDER BY OFFSET(ID(n)));",
        tableName, tableName, tableName);
    context->runQuery(query);
    query = common::stringFormat(
        "CREATE NODE TABLE {}_dict (term STRING, df UINT64, PRIMARY KEY(term))", tableName);
    context->runQuery(query);
    query = common::stringFormat("COPY {}_dict FROM "
                                 "(MATCH (t:{}_terms_list) "
                                 "RETURN t.term, CAST(count(distinct t.offset) AS UINT64))",
        tableName, tableName);
    context->runQuery(query);
    query = common::stringFormat(
        "CREATE REL TABLE {}_terms (FROM {}_dict TO {}_docs, tf UINT64, MANY_MANY);", tableName,
        tableName, tableName);
    context->runQuery(query);
    query = common::stringFormat("COPY {}_terms FROM ("
                                 "MATCH (b:{}_terms_list) "
                                 "RETURN b.term, b.offset, CAST(count(*) as UINT64));",
        tableName, tableName);
    context->runQuery(query);
    query = common::stringFormat(
        "CREATE NODE TABLE {}_stats (ID SERIAL, num_docs UINT64, avg_dl DOUBLE, PRIMARY KEY(ID))",
        tableName);
    context->runQuery(query);
    query = common::stringFormat("COPY {}_stats FROM (MATCH (d:{}_docs) "
                                 "RETURN CAST(count(d) AS UINT64), "
                                 "CAST(SUM(d.len) AS DOUBLE) / CAST(COUNT(d.len) AS DOUBLE));",
        tableName, tableName);
    context->runQuery(query);
    return 1;
}

function_set CreateFTSFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState,
        initEmptyLocalState,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::LIST});
    func->canParallelFunc = []() { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace fts_extension
} // namespace kuzu
