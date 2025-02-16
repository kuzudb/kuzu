#include "function/create_fts_index.h"

#include "binder/expression/literal_expression.h"
#include "catalog/fts_index_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/types/value/nested.h"
#include "fts_extension.h"
#include "function/fts_bind_data.h"
#include "function/fts_config.h"
#include "function/fts_utils.h"
#include "function/gds/gds_task.h"
#include "function/gds/gds_utils.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/table_function.h"
#include "graph/graph_entry.h"
#include "graph/on_disk_graph.h"
#include "processor/execution_context.h"
#include "storage/index/index_utils.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::function;

struct CreateFTSBindData final : FTSBindData {
    std::vector<property_id_t> propertyIDs;
    CreateFTSConfig createFTSConfig;

    CreateFTSBindData(std::string tableName, table_id_t tableID, std::string indexName,
        std::vector<property_id_t> propertyIDs, CreateFTSConfig createFTSConfig)
        : FTSBindData{std::move(tableName), tableID, std::move(indexName),
              binder::expression_vector{}},
          propertyIDs{std::move(propertyIDs)}, createFTSConfig{std::move(createFTSConfig)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CreateFTSBindData>(*this);
    }
};

static std::vector<property_id_t> bindProperties(const catalog::NodeTableCatalogEntry& entry,
    const std::shared_ptr<binder::Expression>& properties) {
    auto propertyValue = properties->constPtrCast<binder::LiteralExpression>()->getValue();
    std::vector<property_id_t> result;
    for (auto i = 0u; i < propertyValue.getChildrenSize(); i++) {
        auto propertyName = NestedVal::getChildVal(&propertyValue, i)->toString();
        if (!entry.containsProperty(propertyName)) {
            throw BinderException{stringFormat("Property: {} does not exist in table {}.",
                propertyName, entry.getName())};
        }
        if (entry.getProperty(entry.getPropertyID(propertyName)).getType() !=
            LogicalType::STRING()) {
            throw BinderException{"Full text search index can only be built on string properties."};
        }
        result.push_back(entry.getPropertyID(propertyName));
    }
    return result;
}

static void validateInternalTableNotExist(const std::string& tableName,
    const catalog::Catalog& catalog, const transaction::Transaction* transaction) {
    if (catalog.containsTable(transaction, tableName)) {
        throw BinderException{
            stringFormat("Table: {} already exists. Please drop or rename the table before "
                         "creating a full text search index.",
                tableName)};
    }
}

static void validateInternalTablesNotExist(table_id_t tableID, const std::string& indexName,
    const catalog::Catalog& catalog, const transaction::Transaction* transaction) {
    validateInternalTableNotExist(FTSUtils::getDocsTableName(tableID, indexName), catalog,
        transaction);
    validateInternalTableNotExist(FTSUtils::getAppearsInTableName(tableID, indexName), catalog,
        transaction);
    validateInternalTableNotExist(FTSUtils::getAppearsInfoTableName(tableID, indexName), catalog,
        transaction);
    validateInternalTableNotExist(FTSUtils::getTermsTableName(tableID, indexName), catalog,
        transaction);
}

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    const TableFuncBindInput* input) {
    storage::IndexUtils::validateAutoTransaction(*context, CreateFTSFunction::name);
    auto indexName = input->getLiteralVal<std::string>(1);
    auto nodeTableEntry = storage::IndexUtils::bindTable(*context,
        input->getLiteralVal<std::string>(0), indexName, storage::IndexOperation::CREATE);
    auto propertyIDs = bindProperties(*nodeTableEntry, input->getParam(2));
    auto createFTSConfig =
        CreateFTSConfig{*context, nodeTableEntry->getTableID(), indexName, input->optionalParams};
    return std::make_unique<CreateFTSBindData>(nodeTableEntry->getName(),
        nodeTableEntry->getTableID(), indexName, std::move(propertyIDs),
        std::move(createFTSConfig));
}

static std::string createStopWordsTable(const ClientContext& context,
    const StopWordsTableInfo& info) {
    std::string query = "";
    auto catalog = context.getCatalog();
    switch (info.source) {
    case StopWordsSource::DEFAULT: {
        if (catalog->containsTable(context.getTransaction(), info.tableName)) {
            return query;
        }
        query +=
            stringFormat("CREATE NODE TABLE `{}` (sw STRING, PRIMARY KEY(sw));", info.tableName);
        for (auto i = 0u; i < FTSExtension::NUM_STOP_WORDS; i++) {
            query += stringFormat("CREATE (s:`{}` {sw: \"{}\"});", info.tableName,
                FTSExtension::EN_STOP_WORDS[i]);
        }
    } break;
    case StopWordsSource::TABLE: {
        query +=
            stringFormat("CREATE NODE TABLE `{}` (sw STRING, PRIMARY KEY(sw));", info.tableName);
        query += stringFormat("COPY `{}` FROM (MATCH (c:`{}`) RETURN c.*);", info.tableName,
            info.stopWords);
    } break;
    case StopWordsSource::FILE: {
        query +=
            stringFormat("CREATE NODE TABLE `{}` (sw STRING, PRIMARY KEY(sw));", info.tableName);
        query += stringFormat("COPY `{}` FROM '{}';", info.tableName, info.stopWords);
    } break;
    default:
        KU_UNREACHABLE;
    }
    return query;
}

std::string createFTSIndexQuery(ClientContext& context, const TableFuncBindData& bindData) {
    auto ftsBindData = bindData.constPtrCast<CreateFTSBindData>();
    auto tableID = ftsBindData->tableID;
    auto indexName = ftsBindData->indexName;
    validateInternalTablesNotExist(ftsBindData->tableID, ftsBindData->indexName,
        *context.getCatalog(), context.getTransaction());
    context.setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    // TODO(Ziyi): Copy statement can't be wrapped in manual transaction, so we can't wrap all
    // statements in a single transaction there.
    // Create the tokenize macro.
    std::string query = "";
    if (!context.getCatalog()->containsMacro(context.getTransaction(), "tokenize")) {
        query += R"(CREATE MACRO tokenize(query) AS
                            string_split(lower(regexp_replace(
                            CAST(query as STRING),
                            '[0-9!@#$%^&*()_+={}\\[\\]:;<>,.?~\\\\/\\|\'"`-]+',
                            ' ',
                            'g')), ' ');)";
    }

    // Create the stop words table if not exists, or the user is not using the default english
    // one.
    query += createStopWordsTable(context, ftsBindData->createFTSConfig.stopWordsTableInfo);

    // Create the terms_in_doc table which servers as a temporary table to store the
    // relationship between terms and docs.
    auto appearsInfoTableName = FTSUtils::getAppearsInfoTableName(tableID, indexName);
    query += stringFormat("CREATE NODE TABLE `{}` (ID SERIAL, term string, docID INT64, primary "
                          "key(ID));",
        appearsInfoTableName);
    auto tableName = ftsBindData->tableName;
    auto tableEntry =
        context.getCatalog()->getTableCatalogEntry(context.getTransaction(), tableName);
    for (auto& property : ftsBindData->propertyIDs) {
        auto propertyName = tableEntry->getProperty(property).getName();
        query += stringFormat("COPY `{}` FROM "
                              "(MATCH (b:`{}`) "
                              "WITH tokenize(b.{}) AS tk, OFFSET(ID(b)) AS id "
                              "UNWIND tk AS t "
                              "WITH t AS t1, id AS id1 "
                              "WHERE t1 is NOT NULL AND SIZE(t1) > 0 AND "
                              "NOT EXISTS {MATCH (s:`{}` {sw: t1})} "
                              "RETURN STEM(t1, '{}'), id1);",
            appearsInfoTableName, tableName, propertyName,
            ftsBindData->createFTSConfig.stopWordsTableInfo.tableName,
            ftsBindData->createFTSConfig.stemmer);
    }

    auto docsTableName = FTSUtils::getDocsTableName(tableID, indexName);
    // Create the docs table which records the number of words in each document.
    query += stringFormat("CREATE NODE TABLE `{}` (docID INT64, len UINT64, primary key(docID));",
        docsTableName);
    query += stringFormat("COPY `{}` FROM "
                          "(MATCH (t:`{}`) "
                          "RETURN t.docID, CAST(count(t) AS UINT64)); ",
        docsTableName, appearsInfoTableName);

    auto termsTableName = FTSUtils::getTermsTableName(tableID, indexName);
    // Create the dic table which records all distinct terms and their document frequency.
    query += stringFormat("CREATE NODE TABLE `{}` (term STRING, df UINT64, PRIMARY KEY(term));",
        termsTableName);
    query += stringFormat("COPY `{}` FROM "
                          "(MATCH (t:`{}`) "
                          "RETURN t.term, CAST(count(distinct t.docID) AS UINT64));",
        termsTableName, appearsInfoTableName);

    auto appearsInTableName = FTSUtils::getAppearsInTableName(tableID, indexName);
    // Finally, create a terms table that records the documents in which the terms appear, along
    // with the frequency of each term.
    query += stringFormat("CREATE REL TABLE `{}` (FROM `{}` TO `{}`, tf UINT64) WITH "
                          "(storage_direction = 'fwd');",
        appearsInTableName, termsTableName, docsTableName);
    query += stringFormat("COPY `{}` FROM ("
                          "MATCH (b:`{}`) "
                          "RETURN b.term, b.docID, CAST(count(*) as UINT64));",
        appearsInTableName, appearsInfoTableName);

    // Drop the intermediate terms_in_doc table.
    query += stringFormat("DROP TABLE `{}`;", appearsInfoTableName);
    std::string properties = "[";
    for (auto i = 0u; i < ftsBindData->propertyIDs.size(); i++) {
        properties +=
            stringFormat("'{}'", tableEntry->getProperty(ftsBindData->propertyIDs[i]).getName());
        if (i != ftsBindData->propertyIDs.size() - 1) {
            properties += ", ";
        }
    }
    properties += "]";
    std::string params;
    params += stringFormat("stemmer := '{}', ", ftsBindData->createFTSConfig.stemmer);
    params += stringFormat("stopWords := '{}'",
        ftsBindData->createFTSConfig.stopWordsTableInfo.stopWords);
    query += stringFormat("CALL _CREATE_FTS_INDEX('{}', '{}', {}, {});", tableName, indexName,
        properties, params);
    query += stringFormat("RETURN 'Index {} has been created.' as result;", ftsBindData->indexName);
    return query;
}

class LenComputeSharedState {
public:
    LenComputeSharedState() : totalLen{0}, numDocs{0} {}

    std::atomic<uint64_t> totalLen;
    std::atomic<idx_t> numDocs;
};

class LenCompute final : public VertexCompute {
public:
    explicit LenCompute(LenComputeSharedState* sharedState) : sharedState{sharedState} {}

    void vertexCompute(const graph::VertexScanState::Chunk& chunk) override;

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<LenCompute>(sharedState);
    }

private:
    LenComputeSharedState* sharedState;
};

void LenCompute::vertexCompute(const graph::VertexScanState::Chunk& chunk) {
    auto nodeIDs = chunk.getNodeIDs();
    auto len = chunk.getProperties<uint64_t>(0);
    for (auto i = 0u; i < nodeIDs.size(); i++) {
        sharedState->totalLen += len[i];
        sharedState->numDocs++;
    }
}

// Do vertex compute to get the numDocs and avgDocLen.
static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    auto& bindData = *input.bindData->constPtrCast<CreateFTSBindData>();
    auto& context = *input.context;
    auto docTableName = FTSUtils::getDocsTableName(bindData.tableID, bindData.indexName);
    auto docTableEntry = context.clientContext->getCatalog()->getTableCatalogEntry(
        context.clientContext->getTransaction(), docTableName);
    graph::GraphEntry entry{{docTableEntry}, {} /* relTableEntries */};
    graph::OnDiskGraph graph(context.clientContext, std::move(entry));
    auto sharedState = LenComputeSharedState{};
    LenCompute lenCompute{&sharedState};
    GDSUtils::runVertexCompute(&context, &graph, lenCompute,
        std::vector<std::string>{InternalCreateFTSFunction::DOC_LEN_PROP_NAME});
    auto numDocs = sharedState.numDocs.load();
    auto avgDocLen = numDocs == 0 ? 0 : static_cast<double>(sharedState.totalLen.load()) / numDocs;
    context.clientContext->getCatalog()->createIndex(context.clientContext->getTransaction(),
        std::make_unique<catalog::IndexCatalogEntry>(FTSIndexCatalogEntry::TYPE_NAME,
            bindData.tableID, bindData.indexName, bindData.propertyIDs,
            std::make_unique<FTSIndexAuxInfo>(numDocs, avgDocLen,
                FTSConfig{bindData.createFTSConfig.stemmer,
                    bindData.createFTSConfig.stopWordsTableInfo.tableName,
                    bindData.createFTSConfig.stopWordsTableInfo.stopWords})));
    return 0;
}

function_set InternalCreateFTSFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name,
        std::vector{LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::LIST});
    func->tableFunc = tableFunc;
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = TableFunction::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set CreateFTSFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name,
        std::vector{LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::LIST});
    func->tableFunc = TableFunction::emptyTableFunc;
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = TableFunction::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->rewriteFunc = createFTSIndexQuery;
    func->canParallelFunc = [] { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace fts_extension
} // namespace kuzu
