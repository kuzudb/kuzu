#include "function/query_fts_index.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "catalog/fts_index_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/task_system/task_scheduler.h"
#include "common/types/internal_id_util.h"
#include "function/fts_utils.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_task.h"
#include "function/gds/gds_utils.h"
#include "function/query_fts_bind_data.h"
#include "graph/on_disk_graph.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"
#include "storage/index/index_utils.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace fts_extension {

using namespace storage;
using namespace function;
using namespace binder;
using namespace common;

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
    const std::unordered_map<common::offset_t, uint64_t>& dfs;

    QFTSEdgeCompute(node_id_map_t<ScoreInfo>& scores,
        const std::unordered_map<common::offset_t, uint64_t>& dfs)
        : scores{scores}, dfs{dfs} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& resultChunk,
        bool) override {
        KU_ASSERT(dfs.contains(boundNodeID.offset));
        auto df = dfs.at(boundNodeID.offset);
        std::vector<nodeID_t> activeNodes;
        resultChunk.forEach<uint64_t>([&](auto docNodeID, auto /* edgeID */, auto tf) {
            if (!scores.contains(docNodeID)) {
                scores.emplace(docNodeID, ScoreInfo{});
            }
            scores.at(docNodeID).addEdge(df, tf);
            activeNodes.push_back(docNodeID);
        });
        return activeNodes;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<QFTSEdgeCompute>(scores, dfs);
    }
};

static void initDenseFrontier(PathLengths& frontier, table_id_t termsTableID,
    const std::unordered_map<common::offset_t, uint64_t>& dfs) {
    frontier.pinNextFrontierTableID(termsTableID);
    for (auto& [offset, _] : dfs) {
        frontier.setActive(offset);
    }
}

static void initSparseFrontier(SparseFrontier& frontier, table_id_t termsTableID,
    const std::unordered_map<common::offset_t, uint64_t>& dfs) {
    frontier.pinTableID(termsTableID);
    for (auto& [offset, _] : dfs) {
        frontier.addNode(offset);
        frontier.checkSampleSize();
    }
}

class QFTSOutputWriter {
public:
    QFTSOutputWriter(const node_id_map_t<ScoreInfo>& scores, storage::MemoryManager* mm,
        const QueryFTSBindData& bindData, uint64_t numUniqueTerms);

    void write(processor::FactorizedTable& scoreFT, nodeID_t docNodeID, uint64_t len,
        int64_t docsID);

    std::unique_ptr<QFTSOutputWriter> copy() {
        return std::make_unique<QFTSOutputWriter>(scores, mm, bindData, numUniqueTerms);
    }

private:
    std::unique_ptr<common::ValueVector> createVector(const LogicalType& type);

private:
    const node_id_map_t<ScoreInfo>& scores;
    storage::MemoryManager* mm;
    const QueryFTSBindData& bindData;

    std::unique_ptr<ValueVector> docsVector;
    std::unique_ptr<ValueVector> scoreVector;
    std::vector<ValueVector*> vectors;
    uint64_t numUniqueTerms;
};

QFTSOutputWriter::QFTSOutputWriter(const node_id_map_t<ScoreInfo>& scores,
    storage::MemoryManager* mm, const QueryFTSBindData& bindData, uint64_t numUniqueTerms)
    : scores{scores}, mm{mm}, bindData{bindData}, numUniqueTerms{numUniqueTerms} {
    docsVector = createVector(LogicalType::INTERNAL_ID());
    scoreVector = createVector(LogicalType::UINT64());
}

std::unique_ptr<common::ValueVector> QFTSOutputWriter::createVector(const LogicalType& type) {
    auto vector = std::make_unique<ValueVector>(type.copy(), mm);
    vector->state = DataChunkState::getSingleValueDataChunkState();
    vectors.push_back(vector.get());
    return vector;
}

void QFTSOutputWriter::write(processor::FactorizedTable& scoreFT, nodeID_t docNodeID, uint64_t len,
    int64_t docsID) {
    auto k = bindData.config.k;
    auto b = bindData.config.b;

    if (!scores.contains(docNodeID)) {
        return;
    }
    auto scoreInfo = scores.at(docNodeID);
    double score = 0;
    // If the query is conjunctive, the numbers of distinct terms in the doc and the number of
    // distinct terms in the query must be equal to each other.
    if (bindData.config.isConjunctive && scoreInfo.scoreData.size() != numUniqueTerms) {
        return;
    }
    auto auxInfo = bindData.entry.getAuxInfo().cast<FTSIndexAuxInfo>();
    for (auto& scoreData : scoreInfo.scoreData) {
        auto numDocs = auxInfo.numDocs;
        auto avgDocLen = auxInfo.avgDocLen;
        auto df = scoreData.df;
        auto tf = scoreData.tf;
        score += log10((numDocs - df + 0.5) / (df + 0.5) + 1) *
                 ((tf * (k + 1) / (tf + k * (1 - b + b * (len / avgDocLen)))));
    }
    docsVector->setValue(0, nodeID_t{static_cast<offset_t>(docsID), bindData.outputTableID});
    scoreVector->setValue(0, score);
    scoreFT.append(vectors);
}

class QFTSVertexCompute final : public VertexCompute {
public:
    QFTSVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        std::unique_ptr<QFTSOutputWriter> writer)
        : mm{mm}, sharedState{sharedState}, writer{std::move(writer)} {
        localFT = sharedState->claimLocalTable(mm);
    }

    ~QFTSVertexCompute() override { sharedState->returnLocalTable(localFT); }

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

private:
    storage::MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    processor::FactorizedTable* localFT;
    std::unique_ptr<QFTSOutputWriter> writer;
};

static std::unordered_map<common::offset_t, uint64_t> getDFs(main::ClientContext& context,
    const catalog::NodeTableCatalogEntry& termsEntry, const std::vector<std::string>& terms) {
    auto storageManager = context.getStorageManager();
    auto tableID = termsEntry.getTableID();
    auto& termsNodeTable = storageManager->getTable(tableID)->cast<storage::NodeTable>();
    auto tx = context.getTransaction();
    auto dfColumnID = termsEntry.getColumnID(QueryFTSAlgorithm::DOC_FREQUENCY_PROP_NAME);
    auto dfColumn = &termsNodeTable.getColumn(dfColumnID);
    std::vector<common::LogicalType> vectorTypes;
    vectorTypes.push_back(LogicalType::UINT64());
    vectorTypes.push_back(LogicalType::INTERNAL_ID());
    auto dataChunk =
        storage::Table::constructDataChunk(context.getMemoryManager(), std::move(vectorTypes));
    dataChunk.state->getSelVectorUnsafe().setSelSize(1);
    auto dfVector = &dataChunk.getValueVector(0);
    auto nodeIDVector = &dataChunk.getValueVectorMutable(1);
    auto termsVector = ValueVector(LogicalType::STRING(), context.getMemoryManager());
    termsVector.state = dataChunk.state;
    auto nodeTableScanState =
        storage::NodeTableScanState{tableID, {dfColumnID}, {dfColumn}, dataChunk, nodeIDVector};
    std::unordered_map<common::offset_t, uint64_t> dfs;
    for (auto& term : terms) {
        termsVector.setValue(0, term);
        offset_t offset = 0;
        if (!termsNodeTable.lookupPK(tx, &termsVector, 0 /* vectorPos */, offset)) {
            continue;
        }
        auto nodeID = nodeID_t{offset, tableID};
        nodeIDVector->setValue(0, nodeID);
        termsNodeTable.initScanState(tx, nodeTableScanState, tableID, offset);
        termsNodeTable.lookup(tx, nodeTableScanState);
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

void QueryFTSAlgorithm::exec(processor::ExecutionContext* executionContext) {
    auto clientContext = executionContext->clientContext;
    auto transaction = clientContext->getTransaction();
    auto graph = sharedState->graph.get();
    auto graphEntry = graph->getGraphEntry();
    auto qFTSBindData = bindData->ptrCast<QueryFTSBindData>();
    auto& termsEntry = graphEntry->nodeEntries[0]->constCast<catalog::NodeTableCatalogEntry>();
    auto terms = bindData->ptrCast<QueryFTSBindData>()->getTerms(*executionContext->clientContext);
    auto dfs = getDFs(*executionContext->clientContext, termsEntry, terms);
    // Do edge compute to extend terms -> docs and save the term frequency and document frequency
    // for each term-doc pair. The reason why we store the term frequency and document frequency
    // is that: we need the `len` property from the docs table which is only available during the
    // vertex compute.
    auto currentFrontier = getPathLengthsFrontier(executionContext, PathLengths::UNVISITED);
    auto nextFrontier = getPathLengthsFrontier(executionContext, PathLengths::UNVISITED);
    auto frontierPair =
        std::make_unique<DoublePathLengthsFrontierPair>(currentFrontier, nextFrontier);
    auto termsTableID = termsEntry.getTableID();
    frontierPair->pinNextFrontier(termsTableID);
    initDenseFrontier(*nextFrontier, termsTableID, dfs);
    auto storageManager = clientContext->getStorageManager();
    auto totalNumTerms = storageManager->getTable(termsTableID)->getNumTotalRows(transaction);
    auto sparseFrontier = SparseFrontier(getSparseFrontierSize(totalNumTerms));
    initSparseFrontier(sparseFrontier, termsTableID, dfs);
    frontierPair->mergeLocalFrontier(sparseFrontier);
    frontierPair->setActiveNodesForNextIter();

    node_id_map_t<ScoreInfo> scores;
    auto edgeCompute = std::make_unique<QFTSEdgeCompute>(scores, dfs);
    auto compState = GDSComputeState(std::move(frontierPair), std::move(edgeCompute),
        nullptr /* outputNodeMask */);
    GDSUtils::runFrontiersUntilConvergence(executionContext, compState, graph, ExtendDirection::FWD,
        1 /* maxIters */, QueryFTSAlgorithm::TERM_FREQUENCY_PROP_NAME);

    // Do vertex compute to calculate the score for doc with the length property.
    auto mm = clientContext->getMemoryManager();
    auto numUniqueTerms = getNumUniqueTerms(terms);
    auto writer = std::make_unique<QFTSOutputWriter>(scores, mm, *qFTSBindData, numUniqueTerms);
    auto vc = std::make_unique<QFTSVertexCompute>(mm, sharedState.get(), std::move(writer));
    auto vertexPropertiesToScan = std::vector<std::string>{QueryFTSAlgorithm::DOC_LEN_PROP_NAME,
        QueryFTSAlgorithm::DOC_ID_PROP_NAME};
    auto docsEntry = graphEntry->nodeEntries[1];
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
        GDSUtils::runVertexCompute(executionContext, graph, *vc, docsEntry, vertexPropertiesToScan);
    }
    sharedState->mergeLocalTables();
}

expression_vector QueryFTSAlgorithm::getResultColumns(
    const function::GDSBindInput& bindInput) const {
    expression_vector columns;
    auto& docsNode = bindData->getNodeOutput()->constCast<NodeExpression>();
    columns.push_back(docsNode.getInternalID());
    std::string scoreColumnName = QueryFTSAlgorithm::SCORE_PROP_NAME;
    if (!bindInput.yieldVariables.empty()) {
        scoreColumnName = bindColumnName(bindInput.yieldVariables[1], scoreColumnName);
    }
    auto scoreColumn = bindInput.binder->createVariable(scoreColumnName, LogicalType::DOUBLE());
    columns.push_back(scoreColumn);
    return columns;
}

static std::string getParamVal(const GDSBindInput& input, idx_t idx) {
    if (input.getParam(idx)->expressionType != ExpressionType::LITERAL) {
        throw BinderException{"The table and index name must be literal expressions."};
    }
    return ExpressionUtil::getLiteralValue<std::string>(
        input.getParam(idx)->constCast<LiteralExpression>());
}

void QueryFTSAlgorithm::bind(const GDSBindInput& input, main::ClientContext& context) {
    context.setToUseInternalCatalogEntry();
    // For queryFTS, the table and index name must be given at compile time while the user
    // can give the query at runtime.
    auto inputTableName = getParamVal(input, 0);
    auto indexName = getParamVal(input, 1);
    auto query = input.getParam(2);

    auto tableEntry = storage::IndexUtils::bindTable(context, inputTableName, indexName,
        storage::IndexOperation::QUERY);
    auto ftsIndexEntry = context.getCatalog()->getIndex(context.getTransaction(),
        tableEntry->getTableID(), indexName);
    auto entry =
        context.getCatalog()->getTableCatalogEntry(context.getTransaction(), inputTableName);
    auto nodeOutput = bindNodeOutput(input, {entry});

    auto termsEntry = context.getCatalog()->getTableCatalogEntry(context.getTransaction(),
        FTSUtils::getTermsTableName(tableEntry->getTableID(), indexName));
    auto docsEntry = context.getCatalog()->getTableCatalogEntry(context.getTransaction(),
        FTSUtils::getDocsTableName(tableEntry->getTableID(), indexName));
    auto appearsInEntry = context.getCatalog()->getTableCatalogEntry(context.getTransaction(),
        FTSUtils::getAppearsInTableName(tableEntry->getTableID(), indexName));
    auto graphEntry = graph::GraphEntry({termsEntry, docsEntry}, {appearsInEntry});
    bindData = std::make_unique<QueryFTSBindData>(std::move(graphEntry), nodeOutput,
        std::move(query), *ftsIndexEntry, QueryFTSConfig{input.optionalParams});
}

function_set QueryFTSFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<QueryFTSAlgorithm>();
    result.push_back(
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo)));
    return result;
}

} // namespace fts_extension
} // namespace kuzu
