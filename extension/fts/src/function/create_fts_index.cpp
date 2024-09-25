#include "function/create_fts_index.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "fts_extension.h"
#include "function/table/bind_input.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::function;

struct CreateFTSBindData final : public CallTableFuncBindData {

    std::string tableName;

    CreateFTSBindData(std::string tableName, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, offset_t maxOffset)
        : tableName{std::move(tableName)},
          CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CreateFTSBindData>(tableName, LogicalType::copy(columnTypes),
            columnNames, maxOffset);
    }
};

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    ScanTableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.push_back("");
    columnTypes.push_back(LogicalType::STRING());
    auto tableName = input->inputs[0].toString();
    if (!context->getCatalog()->containsTable(context->getTx(), tableName)) {
        throw BinderException{common::stringFormat("Table {} does not exist.", tableName)};
    }
    return std::make_unique<CreateFTSBindData>(tableName, std::move(columnTypes),
        std::move(columnNames), 1 /* one row result */);
}

static common::offset_t tableFunc(TableFuncInput& data, TableFuncOutput& output) {
    auto bindData = data.bindData->constPtrCast<CreateFTSBindData>();
    auto& dataChunk = output.dataChunk;
    auto sharedState = data.sharedState->ptrCast<CallFuncSharedState>();
    auto outputVector = dataChunk.getValueVector(0);
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        return 0;
    }
    auto context = data.context;
    auto catalog = context->getCatalog();
    auto tableName = bindData->tableName;
    auto table = catalog->getTableCatalogEntry(context->getTx(), tableName)
                     ->constPtrCast<catalog::NodeTableCatalogEntry>();
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

    auto& pk = table->getPrimaryKeyDefinition();
    query = common::stringFormat("CREATE NODE TABLE {}_docs (docID {}, primary key(docID))",
        tableName, pk.getType().toString());
    context->runQuery(query);
    query = common::stringFormat("COPY {}_docs FROM (MATCH (n:{}) RETURN n.{})", tableName,
        tableName, pk.getName());
    context->runQuery(query);

    query = common::stringFormat(
        "CREATE NODE TABLE {}_terms_list (ID SERIAL, term string, docID {}, primary key(ID))",
        tableName, pk.getType().toString());
    context->runQuery(query);
    // clang-format off
    query =
        common::stringFormat("COPY {}_terms_list FROM "
                             "(MATCH (b:books) "
                             "WITH tokenize(b.content) AS tk, b.id AS id "
                             "UNWIND tk AS t "
                             "WITH t AS t1, id AS id1 "
                             "WHERE t1 is NOT NULL AND SIZE(t1) > 0 AND NOT EXISTS {MATCH (s:{}_stopwords {sw: t1})} "
                             "RETURN STEM(t1, 'porter'), id1);",
            tableName, tableName);
    // clang-format on
    context->runQuery(query);
    query = common::stringFormat("ALTER TABLE {}_docs ADD len UINT64;", tableName);
    context->runQuery(query);
    query = common::stringFormat(
        "MATCH (doc:{}_docs) "
        "WITH COUNT{MATCH (term:{}_terms_list {docID: doc.docID})} AS len, doc as doc1 "
        "SET doc1.len = len",
        tableName, tableName);
    context->runQuery(query);

    query = common::stringFormat(
        "CREATE NODE TABLE {}_dict (term STRING, df UINT64, PRIMARY KEY(term))", tableName);
    context->runQuery(query);
    query = common::stringFormat("COPY {}_dict FROM "
                                 "(MATCH (t:{}_terms_list) "
                                 "RETURN t.term, CAST(count(distinct t.docID) AS UINT64))",
        tableName, tableName);
    context->runQuery(query);

    query = common::stringFormat("CREATE REL TABLE {}_terms (FROM {}_dict TO {}_docs, MANY_MANY);",
        tableName, tableName, tableName);
    context->runQuery(query);
    query = common::stringFormat(
        "COPY {}_terms FROM (MATCH (b:books_terms_list) RETURN b.term, b.docID);", tableName);
    context->runQuery(query);

    query = common::stringFormat(
        "CREATE NODE TABLE {}_stats (ID SERIAL, num_docs UINT64, avg_dl DOUBLE, PRIMARY KEY(ID))",
        tableName);
    context->runQuery(query);
    query = common::stringFormat("COPY {}_stats FROM (MATCH (d:{}_docs) "
                                 "RETURN CAST(count(d.docID) AS UINT64), "
                                 "CAST(SUM(d.len) AS DOUBLE) / CAST(COUNT(d.len) AS DOUBLE));",
        tableName, tableName);
    context->runQuery(query);
    return 1;
}

function_set CreateFTSFunction::getFunctionSet() {
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
