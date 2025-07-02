#include "function/query_fts_index.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "binder/query/reading_clause/bound_table_function_call.h"
#include "catalog/fts_index_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/types/internal_id_util.h"
#include "function/fts_index_utils.h"
#include "function/gds/gds_utils.h"
#include "function/query_fts_bind_data.h"
#include "index/fts_index.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/planner.h"
#include "processor/execution_context.h"
#include "storage/storage_manager.h"
#include "storage/table/node_table.h"
#include "utils/fts_utils.h"

namespace kuzu {
namespace fts_extension {

using namespace storage;
using namespace function;
using namespace binder;
using namespace common;
using namespace planner;

struct ScoreData {
    uint64_t df;
    uint64_t tf;

    ScoreData(uint64_t df, uint64_t tf) : df{df}, tf{tf} {}
};

struct ScoreInfo {
    std::vector<ScoreData> scoreData;

    void addEdge(uint64_t df, uint64_t tf) { scoreData.emplace_back(df, tf); }
};

struct QFTSEdgeCompute final : EdgeCompute {
    node_id_map_t<ScoreInfo>& scores;
    const std::unordered_map<offset_t, uint64_t>& dfs;

    QFTSEdgeCompute(node_id_map_t<ScoreInfo>& scores,
        const std::unordered_map<offset_t, uint64_t>& dfs)
        : scores{scores}, dfs{dfs} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& resultChunk,
        bool) override {
        KU_ASSERT(dfs.contains(boundNodeID.offset));
        auto df = dfs.at(boundNodeID.offset);
        std::vector<nodeID_t> activeNodes;
        resultChunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
            auto docNodeID = neighbors[i];
            if (!scores.contains(docNodeID)) {
                scores.emplace(docNodeID, ScoreInfo{});
            }
            auto tf = propertyVectors[0]->template getValue<uint64_t>(i);
            scores.at(docNodeID).addEdge(df, tf);
            activeNodes.push_back(docNodeID);
        });
        return activeNodes;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<QFTSEdgeCompute>(scores, dfs);
    }
};

struct QFTSOutputSharedState {
    std::vector<std::pair<common::offset_t, double>>& outputScores;
    std::mutex mtx;

    QFTSOutputSharedState(std::vector<std::pair<common::offset_t, double>>& outputScores)
        : outputScores{outputScores} {}

    void addScore(offset_t docID, double score) {
        std::lock_guard guard{mtx};
        outputScores.emplace_back(docID, score);
    }
};

class QFTSOutputWriter {
public:
    QFTSOutputWriter(const node_id_map_t<ScoreInfo>& scores, MemoryManager* mm,
        QueryFTSConfig config, const QueryFTSBindData& bindData, uint64_t numUniqueTerms,
        std::shared_ptr<QFTSOutputSharedState> sharedState);

    void write(processor::FactorizedTable& scoreFT, nodeID_t docNodeID, uint64_t len,
        int64_t docsID);

    void output(processor::FactorizedTable& table, int64_t top) {
        auto& outputScores = sharedState->outputScores;
        if ((uint64_t)top < outputScores.size()) {
            std::partial_sort(outputScores.begin(), outputScores.begin() + top, outputScores.end(),
                [](const auto& left, const auto& right) { return left.second > right.second; });
            outputScores.resize(top);
        }
        for (auto& [offset, score] : sharedState->outputScores) {
            docsVector->setValue(0, nodeID_t{offset, bindData.outputTableID});
            scoreVector->setValue(0, score);
            table.append(vectors);
        }
    }

    std::unique_ptr<QFTSOutputWriter> copy() {
        return std::make_unique<QFTSOutputWriter>(scores, mm, config, bindData, numUniqueTerms,
            sharedState);
    }

private:
    std::unique_ptr<ValueVector> createVector(const LogicalType& type);

public:
    std::shared_ptr<QFTSOutputSharedState> sharedState;

private:
    const node_id_map_t<ScoreInfo>& scores;
    MemoryManager* mm;
    QueryFTSConfig config;
    const QueryFTSBindData& bindData;

    std::unique_ptr<ValueVector> docsVector;
    std::unique_ptr<ValueVector> scoreVector;
    std::vector<ValueVector*> vectors;
    uint64_t numUniqueTerms;
};

QFTSOutputWriter::QFTSOutputWriter(const node_id_map_t<ScoreInfo>& scores, MemoryManager* mm,
    QueryFTSConfig config, const QueryFTSBindData& bindData, uint64_t numUniqueTerms,
    std::shared_ptr<QFTSOutputSharedState> sharedState)
    : sharedState{std::move(sharedState)}, scores{scores}, mm{mm}, config{config},
      bindData{bindData}, numUniqueTerms{numUniqueTerms} {
    docsVector = createVector(LogicalType::INTERNAL_ID());
    scoreVector = createVector(LogicalType::UINT64());
}

std::unique_ptr<ValueVector> QFTSOutputWriter::createVector(const LogicalType& type) {
    auto vector = std::make_unique<ValueVector>(type.copy(), mm);
    vector->state = DataChunkState::getSingleValueDataChunkState();
    vectors.push_back(vector.get());
    return vector;
}

void QFTSOutputWriter::write(processor::FactorizedTable& scoreFT, nodeID_t docNodeID, uint64_t len,
    int64_t docsID) {
    auto k = config.k;
    auto b = config.b;

    if (!scores.contains(docNodeID)) {
        return;
    }
    auto scoreInfo = scores.at(docNodeID);
    double score = 0;
    // If the query is conjunctive, the numbers of distinct terms in the doc and the number of
    // distinct terms in the query must be equal to each other.
    if (config.isConjunctive && scoreInfo.scoreData.size() != numUniqueTerms) {
        return;
    }
    auto auxInfo = bindData.entry.getAuxInfo().cast<FTSIndexAuxInfo>();
    for (auto& scoreData : scoreInfo.scoreData) {
        auto numDocs = bindData.numDocs;
        auto avgDocLen = bindData.avgDocLen;
        auto df = scoreData.df;
        auto tf = scoreData.tf;
        score += log10((numDocs - df + 0.5) / (df + 0.5) + 1) *
                 ((tf * (k + 1) / (tf + k * (1 - b + b * (len / avgDocLen)))));
    }
    sharedState->addScore(docsID, score);
}

class QFTSVertexCompute final : public VertexCompute {
public:
    QFTSVertexCompute(MemoryManager* mm, GDSFuncSharedState* sharedState,
        std::unique_ptr<QFTSOutputWriter> writer)
        : mm{mm}, sharedState{sharedState}, writer{std::move(writer)} {
        localFT = sharedState->factorizedTablePool.claimLocalTable(mm);
    }

    ~QFTSVertexCompute() override { sharedState->factorizedTablePool.returnLocalTable(localFT); }

    void vertexCompute(const graph::VertexScanState::Chunk& chunk) override {
        auto docLens = chunk.getProperties<uint64_t>(0);
        auto docIDs = chunk.getProperties<int64_t>(1);
        for (auto i = 0u; i < chunk.getNodeIDs().size(); i++) {
            writer->write(*localFT, chunk.getNodeIDs()[i], docLens[i], docIDs[i]);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<QFTSVertexCompute>(mm, sharedState, writer->copy());
    }

    QFTSOutputWriter* getWriter() { return writer.get(); }

    processor::FactorizedTable* getLocalFT() { return localFT; };

private:
    MemoryManager* mm;
    GDSFuncSharedState* sharedState;
    processor::FactorizedTable* localFT;
    std::unique_ptr<QFTSOutputWriter> writer;
};

static constexpr char SCORE_PROP_NAME[] = "score";
static constexpr char DOC_FREQUENCY_PROP_NAME[] = "df";
static constexpr char TERM_FREQUENCY_PROP_NAME[] = "tf";
static constexpr char DOC_LEN_PROP_NAME[] = "len";
static constexpr char DOC_ID_PROP_NAME[] = "docID";

static std::unordered_map<offset_t, uint64_t> getDFs(main::ClientContext& context,
    const catalog::NodeTableCatalogEntry& termsEntry, const std::vector<std::string>& terms) {
    auto storageManager = context.getStorageManager();
    auto tableID = termsEntry.getTableID();
    auto& termsNodeTable = storageManager->getTable(tableID)->cast<NodeTable>();
    auto tx = context.getTransaction();
    auto dfColumnID = termsEntry.getColumnID(DOC_FREQUENCY_PROP_NAME);
    std::vector<LogicalType> vectorTypes;
    vectorTypes.push_back(LogicalType::INTERNAL_ID());
    vectorTypes.push_back(LogicalType::UINT64());
    auto dataChunk = Table::constructDataChunk(context.getMemoryManager(), std::move(vectorTypes));
    dataChunk.state->getSelVectorUnsafe().setSelSize(1);
    auto nodeIDVector = &dataChunk.getValueVectorMutable(0);
    auto dfVector = &dataChunk.getValueVectorMutable(1);
    auto termsVector = ValueVector(LogicalType::STRING(), context.getMemoryManager());
    termsVector.state = dataChunk.state;
    auto nodeTableScanState =
        NodeTableScanState(nodeIDVector, std::vector{dfVector}, dataChunk.state);
    nodeTableScanState.setToTable(context.getTransaction(), &termsNodeTable, {dfColumnID}, {});
    std::unordered_map<offset_t, uint64_t> dfs;
    for (auto& term : terms) {
        termsVector.setValue(0, term);
        offset_t offset = 0;
        if (!termsNodeTable.lookupPK(tx, &termsVector, 0 /* vectorPos */, offset)) {
            continue;
        }
        auto nodeID = nodeID_t{offset, tableID};
        nodeIDVector->setValue(0, nodeID);
        termsNodeTable.initScanState(tx, nodeTableScanState, tableID, offset);
        [[maybe_unused]] auto res = termsNodeTable.lookup(tx, nodeTableScanState);
        dfs.emplace(offset, dfVector->getValue<uint64_t>(0));
    }
    return dfs;
}

static uint64_t getNumUniqueTerms(const std::vector<std::string>& terms) {
    auto uniqueTerms = std::unordered_set<std::string>{terms.begin(), terms.end()};
    return uniqueTerms.size();
}

// A size is considered as sparse if it is smaller than 0.1% of table size of less than 1000.
static constexpr double SPARSE_PERCENTAGE = 0.001;
static constexpr uint64_t MIN_SPARSE_SIZE = 1000;
static uint64_t getSparseFrontierSize(uint64_t numRows) {
    uint64_t size = numRows * SPARSE_PERCENTAGE;
    if (size < MIN_SPARSE_SIZE) {
        return MIN_SPARSE_SIZE;
    }
    return size;
}

static void initFrontier(FrontierPair& frontierPair, table_id_t termsTableID,
    const std::unordered_map<offset_t, uint64_t>& dfs) {
    frontierPair.pinNextFrontier(termsTableID);
    for (auto& [offset, _] : dfs) {
        frontierPair.addNodeToNextFrontier(offset);
    }
}

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    auto clientContext = input.context->clientContext;
    auto transaction = clientContext->getTransaction();
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto graph = sharedState->graph.get();
    auto graphEntry = graph->getGraphEntry();
    auto qFTSBindData = input.bindData->constPtrCast<QueryFTSBindData>();
    auto& termsEntry = graphEntry->nodeInfos[0].entry->constCast<catalog::NodeTableCatalogEntry>();
    auto terms = qFTSBindData->getTerms(*input.context->clientContext);
    auto dfs = getDFs(*input.context->clientContext, termsEntry, terms);
    // Do edge compute to extend terms -> docs and save the term frequency and document frequency
    // for each term-doc pair. The reason why we store the term frequency and document frequency
    // is that: we need the `len` property from the docs table which is only available during the
    // vertex compute.
    auto currentFrontier = DenseFrontier::getUnvisitedFrontier(input.context, graph);
    auto nextFrontier = DenseFrontier::getUnvisitedFrontier(input.context, graph);
    auto frontierPair = std::make_unique<DenseSparseDynamicFrontierPair>(std::move(currentFrontier),
        std::move(nextFrontier));
    auto termsTableID = termsEntry.getTableID();
    initFrontier(*frontierPair, termsTableID, dfs);
    auto storageManager = clientContext->getStorageManager();
    frontierPair->setActiveNodesForNextIter();

    node_id_map_t<ScoreInfo> scores;
    auto edgeCompute = std::make_unique<QFTSEdgeCompute>(scores, dfs);
    auto auxiliaryState = std::make_unique<EmptyGDSAuxiliaryState>();
    auto compState =
        GDSComputeState(std::move(frontierPair), std::move(edgeCompute), std::move(auxiliaryState));
    GDSUtils::runFTSEdgeCompute(input.context, compState, graph, ExtendDirection::FWD,
        {TERM_FREQUENCY_PROP_NAME});

    // Do vertex compute to calculate the score for doc with the length property.
    auto mm = clientContext->getMemoryManager();
    auto numUniqueTerms = getNumUniqueTerms(terms);
    std::vector<std::pair<common::offset_t, double>> finalScores;
    std::shared_ptr<QFTSOutputSharedState> sState =
        std::make_shared<QFTSOutputSharedState>(finalScores);
    auto writer = std::make_unique<QFTSOutputWriter>(scores, mm, qFTSBindData->getConfig(),
        *qFTSBindData, numUniqueTerms, sState);
    auto vc = std::make_unique<QFTSVertexCompute>(mm, sharedState, std::move(writer));
    auto vertexPropertiesToScan = std::vector<std::string>{DOC_LEN_PROP_NAME, DOC_ID_PROP_NAME};
    auto docsEntry = graphEntry->nodeInfos[1].entry;
    auto numDocs = storageManager->getTable(docsEntry->getTableID())->getNumTotalRows(transaction);
    if (scores.size() < getSparseFrontierSize(numDocs)) {
        auto vertexScanState = graph->prepareVertexScan(docsEntry, vertexPropertiesToScan);
        for (auto& [nodeID, scoreInfo] : scores) {
            for (auto chunk :
                graph->scanVertices(nodeID.offset, nodeID.offset + 1, *vertexScanState)) {
                vc->vertexCompute(chunk);
            }
        }
    } else {
        GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vc, docsEntry,
            vertexPropertiesToScan);
    }
    vc->getWriter()->output(*sharedState->factorizedTablePool.getGlobalTable(),
        qFTSBindData->getConfig().top);
    // sharedState->factorizedTablePool.mergeLocalTables();
    return 0;
}

static std::string getParamVal(const TableFuncBindInput& input, idx_t idx) {
    if (input.getParam(idx)->expressionType != ExpressionType::LITERAL) {
        throw BinderException{"The table and index name must be literal expressions."};
    }
    return ExpressionUtil::getLiteralValue<std::string>(
        input.getParam(idx)->constCast<LiteralExpression>());
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    context->setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    // For queryFTS, the table and index name must be given at compile time while the user
    // can give the query at runtime.
    auto inputTableName = getParamVal(*input, 0);
    auto indexName = getParamVal(*input, 1);
    auto query = input->getParam(2);

    auto tableEntry = FTSIndexUtils::bindNodeTable(*context, inputTableName, indexName,
        FTSIndexUtils::IndexOperation::QUERY);
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    auto ftsIndexEntry = catalog->getIndex(transaction, tableEntry->getTableID(), indexName);
    auto entry = catalog->getTableCatalogEntry(transaction, inputTableName);
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, {entry});

    auto termsEntry = catalog->getTableCatalogEntry(transaction,
        FTSUtils::getTermsTableName(tableEntry->getTableID(), indexName));
    auto docsEntry = catalog->getTableCatalogEntry(transaction,
        FTSUtils::getDocsTableName(tableEntry->getTableID(), indexName));
    auto appearsInEntry = catalog->getTableCatalogEntry(transaction,
        FTSUtils::getAppearsInTableName(tableEntry->getTableID(), indexName));
    auto graphEntry = graph::NativeGraphEntry({termsEntry, docsEntry}, {appearsInEntry});

    expression_vector columns;
    auto& docsNode = nodeOutput->constCast<NodeExpression>();
    columns.push_back(docsNode.getInternalID());
    std::string scoreColumnName = SCORE_PROP_NAME;
    if (!input->yieldVariables.empty()) {
        scoreColumnName = GDSFunction::bindColumnName(input->yieldVariables[1], scoreColumnName);
    }
    auto scoreColumn = input->binder->createVariable(scoreColumnName, LogicalType::DOUBLE());
    columns.push_back(scoreColumn);
    auto nodeTable =
        context->getStorageManager()->getTable(ftsIndexEntry->getTableID())->ptrCast<NodeTable>();
    auto index = nodeTable->getIndex(indexName);
    KU_ASSERT(index.has_value());
    auto& ftsIndex = index.value()->cast<FTSIndex>();
    auto& ftsStorageInfo = ftsIndex.getStorageInfo().constCast<FTSStorageInfo>();
    auto bindData =
        std::make_unique<QueryFTSBindData>(std::move(columns), std::move(graphEntry), nodeOutput,
            std::move(query), *ftsIndexEntry, QueryFTSOptionalParams{input->optionalParamsLegacy},
            ftsStorageInfo.numDocs, ftsStorageInfo.avgDocLen);
    context->setUseInternalCatalogEntry(false /* useInternalCatalogEntry */);
    return bindData;
}

static void getLogicalPlan(Planner* planner, const BoundReadingClause& readingClause,
    const expression_vector& predicates, LogicalPlan& plan) {
    auto& call = readingClause.constCast<BoundTableFunctionCall>();
    auto bindData = call.getBindData()->constPtrCast<GDSBindData>();
    auto op = std::make_shared<LogicalTableFunctionCall>(call.getTableFunc(), bindData->copy());
    op->computeFactorizedSchema();
    planner->planReadOp(std::move(op), predicates, plan);

    auto nodeOutput = bindData->nodeOutput->ptrCast<NodeExpression>();
    KU_ASSERT(nodeOutput != nullptr);
    planner->getCardinliatyEstimatorUnsafe().init(*nodeOutput);
    auto scanPlan = planner->getNodePropertyScanPlan(*nodeOutput);
    if (scanPlan.isEmpty()) {
        return;
    }
    expression_vector joinConditions;
    joinConditions.push_back(nodeOutput->getInternalID());
    planner->appendHashJoin(joinConditions, JoinType::INNER, scanPlan, plan, plan);
    plan.getLastOperator()->cast<LogicalHashJoin>().getSIPInfoUnsafe().direction =
        SIPDirection::FORCE_BUILD_TO_PROBE;
}

function_set QueryFTSFunction::getFunctionSet() {
    function_set result;
    // inputs are tableName, indexName, query
    auto func = std::make_unique<TableFunction>(QueryFTSFunction::name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING});
    func->bindFunc = bindFunc;
    func->tableFunc = tableFunc;
    func->initSharedStateFunc = GDSFunction::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    func->getLogicalPlanFunc = getLogicalPlan;
    func->getPhysicalPlanFunc = GDSFunction::getPhysicalPlan;
    result.push_back(std::move(func));
    return result;
}

} // namespace fts_extension
} // namespace kuzu
