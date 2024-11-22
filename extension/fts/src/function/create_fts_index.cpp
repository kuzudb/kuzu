#include "function/create_fts_index.h"

#include "binder/expression/expression_util.h"
#include "catalog/fts_index_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/types/value/nested.h"
#include "fts_extension.h"
#include "function/fts_bind_data.h"
#include "function/fts_utils.h"
#include "function/gds/gds_task.h"
#include "function/gds/gds_utils.h"
#include "function/table/bind_input.h"
#include "graph/graph_entry.h"
#include "graph/on_disk_graph.h"
#include "processor/execution_context.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::function;

struct CreateFTSBindData final : public FTSBindData {
    std::vector<std::string> properties;

    CreateFTSBindData(std::string tableName, common::table_id_t tableID, std::string indexName,
        std::vector<std::string> properties)
        : FTSBindData{std::move(tableName), tableID, std::move(indexName)},
          properties{std::move(properties)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CreateFTSBindData>(*this);
    }
};

static std::vector<std::string> bindProperties(const catalog::NodeTableCatalogEntry& entry,
    const common::Value& properties) {
    std::vector<std::string> result;
    for (auto i = 0u; i < properties.getChildrenSize(); i++) {
        auto propertyName = NestedVal::getChildVal(&properties, i)->toString();
        if (!entry.containsProperty(propertyName)) {
            throw BinderException{common::stringFormat("Property: {} does not exist in table {}.",
                propertyName, entry.getName())};
        }
        result.push_back(std::move(propertyName));
    }
    return result;
}

static void validateIndexNotExist(const main::ClientContext& context, common::table_id_t tableID,
    const std::string& indexName) {
    if (context.getCatalog()->containsIndex(context.getTx(), tableID, indexName)) {
        throw common::BinderException{common::stringFormat("Index: {} already exists in table: {}.",
            indexName, context.getCatalog()->getTableName(context.getTx(), tableID))};
    }
}

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    ScanTableFuncBindInput* input) {
    FTSUtils::validateAutoTrx(*context, CreateFTSFunction::name);
    auto indexName = input->inputs[1].toString();
    auto& nodeTableEntry =
        FTSUtils::bindTable(input->inputs[0], context, indexName, FTSUtils::IndexOperation::CREATE);
    auto properties = bindProperties(nodeTableEntry, input->inputs[2]);
    validateIndexNotExist(*context, nodeTableEntry.getTableID(), indexName);
    return std::make_unique<CreateFTSBindData>(nodeTableEntry.getName(),
        nodeTableEntry.getTableID(), indexName, std::move(properties));
}

std::string createFTSIndexQuery(ClientContext& context, const TableFuncBindData& bindData) {
    auto ftsBindData = bindData.constPtrCast<CreateFTSBindData>();
    // TODO(Ziyi): Copy statement can't be wrapped in manual transaction, so we can't wrap all
    // statements in a single transaction there.
    // Create the tokenize macro.
    std::string query = "";
    if (!context.getCatalog()->containsMacro(context.getTx(), "tokenize")) {
        query += R"(CREATE MACRO tokenize(query) AS
                            string_split(lower(regexp_replace(
                            CAST(query as STRING),
                            '[0-9!@#$%^&*()_+={}\\[\\]:;<>,.?~\\\\/\\|\'"`-]+',
                            ' ',
                            'g')), ' ');)";
    }

    auto stopWordsTableName = ftsBindData->getStopWordsTableName();
    // Create the stop words table.
    query += common::stringFormat("CREATE NODE TABLE `{}` (sw STRING, PRIMARY KEY(sw));",
        stopWordsTableName);
    for (auto i = 0u; i < FTSExtension::NUM_STOP_WORDS; i++) {
        query += common::stringFormat("CREATE (s:`{}` {sw: \"{}\"});", stopWordsTableName,
            FTSExtension::EN_STOP_WORDS[i]);
    }

    // Create the terms_in_doc table which servers as a temporary table to store the relationship
    // between terms and docs.
    auto appearsInfoTableName = ftsBindData->getAppearsInfoTableName();
    query +=
        common::stringFormat("CREATE NODE TABLE `{}` (ID SERIAL, term string, docID INT64, primary "
                             "key(ID));",
            appearsInfoTableName);
    auto tableName = ftsBindData->tableName;
    for (auto& property : ftsBindData->properties) {
        query += common::stringFormat("COPY `{}` FROM "
                                      "(MATCH (b:`{}`) "
                                      "WITH tokenize(b.{}) AS tk, OFFSET(ID(b)) AS id "
                                      "UNWIND tk AS t "
                                      "WITH t AS t1, id AS id1 "
                                      "WHERE t1 is NOT NULL AND SIZE(t1) > 0 AND "
                                      "NOT EXISTS {MATCH (s:`{}` {sw: t1})} "
                                      "RETURN STEM(t1, 'porter'), id1);",
            appearsInfoTableName, tableName, property, stopWordsTableName);
    }

    auto docsTableName = ftsBindData->getDocsTableName();
    // Create the docs table which records the number of words in each document.
    query += common::stringFormat(
        "CREATE NODE TABLE `{}` (docID INT64, len UINT64, primary key(docID));", docsTableName);
    query += common::stringFormat("COPY `{}` FROM "
                                  "(MATCH (t:`{}`) "
                                  "RETURN t.docID, CAST(count(t) AS UINT64)); ",
        docsTableName, appearsInfoTableName);

    auto termsTableName = ftsBindData->getTermsTableName();
    // Create the dic table which records all distinct terms and their document frequency.
    query += common::stringFormat(
        "CREATE NODE TABLE `{}` (term STRING, df UINT64, PRIMARY KEY(term));", termsTableName);
    query += common::stringFormat("COPY `{}` FROM "
                                  "(MATCH (t:`{}`) "
                                  "RETURN t.term, CAST(count(distinct t.docID) AS UINT64));",
        termsTableName, appearsInfoTableName);

    auto appearsInTableName = ftsBindData->getAppearsInTableName();
    // Finally, create a terms table that records the documents in which the terms appear, along
    // with the frequency of each term.
    query +=
        common::stringFormat("CREATE REL TABLE `{}` (FROM `{}` TO `{}`, tf UINT64, MANY_MANY);",
            appearsInTableName, termsTableName, docsTableName);
    query += common::stringFormat("COPY `{}` FROM ("
                                  "MATCH (b:`{}`) "
                                  "RETURN b.term, b.docID, CAST(count(*) as UINT64));",
        appearsInTableName, appearsInfoTableName);

    // Drop the intermediate terms_in_doc table.
    query += common::stringFormat("DROP TABLE `{}`;", appearsInfoTableName);
    return query;
}

class LenComputeSharedState {
public:
    LenComputeSharedState() : totalLen{0}, numDocs{0} {}

    std::atomic<uint64_t> totalLen;
    std::atomic<idx_t> numDocs;
};

class LenCompute : public VertexCompute {
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
static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& /*output*/) {
    auto& createFTSBindData = *input.bindData->constPtrCast<CreateFTSBindData>();
    auto& context = *input.context;
    auto docTableName = createFTSBindData.getDocsTableName();
    auto docTableEntry = context.clientContext->getCatalog()->getTableCatalogEntry(
        context.clientContext->getTx(), docTableName);
    graph::GraphEntry entry{{docTableEntry}, {} /* relTableEntries */};
    graph::OnDiskGraph graph(context.clientContext, std::move(entry));
    auto sharedState = LenComputeSharedState{};
    LenCompute lenCompute{&sharedState};
    GDSUtils::runVertexComputeIteration(&context, &graph, lenCompute,
        std::vector<std::string>{CreateFTSFunction::DOC_LEN_PROP_NAME});
    auto numDocs = sharedState.numDocs.load();
    auto avgDocLen = (double)sharedState.totalLen.load() / numDocs;
    context.clientContext->getCatalog()->createIndex(context.clientContext->getTx(),
        std::make_unique<fts_extension::FTSIndexCatalogEntry>(createFTSBindData.tableID,
            createFTSBindData.indexName, numDocs, avgDocLen));
    return 0;
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
