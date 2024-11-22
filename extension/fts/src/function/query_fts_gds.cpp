#include "function/query_fts_gds.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "catalog/catalog.h"
#include "common/exception/runtime.h"
#include "common/task_system/task_scheduler.h"
#include "common/types/internal_id_util.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_task.h"
#include "function/gds/gds_utils.h"
#include "main/settings.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace fts_extension {

using namespace function;

struct QFTSGDSBindData final : public function::GDSBindData {
    std::shared_ptr<binder::Expression> terms;
    // k: parameter controls the influence of term frequency saturation. It limits the effect of
    // additional occurrences of a term within a document.
    double_t k;
    // b: parameter controls the degree of length normalization by adjusting the influence of
    // document length.
    double_t b;
    uint64_t numDocs;
    double_t avgDocLen;

    QFTSGDSBindData(std::shared_ptr<binder::Expression> terms,
        std::shared_ptr<binder::Expression> docs, double_t k, double_t b, uint64_t numDocs,
        double_t avgDocLen)
        : GDSBindData{std::move(docs)}, terms{std::move(terms)}, k{k}, b{b}, numDocs{numDocs},
          avgDocLen{avgDocLen} {}
    QFTSGDSBindData(const QFTSGDSBindData& other)
        : GDSBindData{other}, terms{other.terms}, k{other.k}, b{other.b}, numDocs{other.numDocs},
          avgDocLen{other.avgDocLen} {}

    bool hasNodeInput() const override { return true; }
    std::shared_ptr<binder::Expression> getNodeInput() const override { return terms; }

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<QFTSGDSBindData>(*this);
    }
};

struct ScoreData {
    uint64_t df;
    uint64_t tf;

    ScoreData(uint64_t df, uint64_t tf) : df{df}, tf{tf} {}
};

struct ScoreInfo {
    nodeID_t termID;
    std::vector<ScoreData> scoreData;

    explicit ScoreInfo(nodeID_t termID) : termID{std::move(termID)} {}

    void addEdge(uint64_t df, uint64_t tf) { scoreData.emplace_back(df, tf); }
};

struct QFTSEdgeCompute : public EdgeCompute {
    DoublePathLengthsFrontierPair* termsFrontier;
    common::node_id_map_t<ScoreInfo>* scores;
    common::node_id_map_t<uint64_t>* dfs;

    QFTSEdgeCompute(DoublePathLengthsFrontierPair* termsFrontier,
        common::node_id_map_t<ScoreInfo>* scores, common::node_id_map_t<uint64_t>* dfs)
        : termsFrontier{termsFrontier}, scores{scores}, dfs{dfs} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& resultChunk,
        bool) override;

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<QFTSEdgeCompute>(termsFrontier, scores, dfs);
    }
};

std::vector<nodeID_t> QFTSEdgeCompute::edgeCompute(nodeID_t boundNodeID,
    graph::NbrScanState::Chunk& resultChunk, bool) {
    KU_ASSERT(dfs->contains(boundNodeID));
    std::vector<nodeID_t> activeNodes;
    resultChunk.forEach<uint64_t>([&](auto docNodeID, auto /* edgeID */, auto tf) {
        auto df = dfs->at(boundNodeID);
        if (!scores->contains(docNodeID)) {
            scores->emplace(docNodeID, ScoreInfo{boundNodeID});
        }
        scores->at(docNodeID).addEdge(df, tf);
        activeNodes.push_back(docNodeID);
    });
    return activeNodes;
}

struct QFTSOutput {
    common::node_id_map_t<ScoreInfo> scores;

    QFTSOutput() = default;
    virtual ~QFTSOutput() = default;
};

struct QFTSState : public function::GDSComputeState {
    void initFirstFrontierWithTerms(processor::GDSCallSharedState& sharedState,
        RoaringBitmapSemiMask& termsMask, transaction::Transaction* tx,
        common::table_id_t termsTableID) const;

    QFTSState(std::unique_ptr<function::FrontierPair> frontierPair,
        std::unique_ptr<function::EdgeCompute> edgeCompute, common::table_id_t termsTableID);
};

QFTSState::QFTSState(std::unique_ptr<function::FrontierPair> frontierPair,
    std::unique_ptr<function::EdgeCompute> edgeCompute, common::table_id_t termsTableID)
    : function::GDSComputeState{std::move(frontierPair), std::move(edgeCompute)} {
    this->frontierPair->getNextFrontierUnsafe().ptrCast<PathLengths>()->pinNextFrontierTableID(
        termsTableID);
}

void QFTSState::initFirstFrontierWithTerms(processor::GDSCallSharedState& sharedState,
    RoaringBitmapSemiMask& termsMask, transaction::Transaction* tx,
    common::table_id_t termsTableID) const {
    auto termNodeID = nodeID_t{INVALID_OFFSET, termsTableID};
    auto numTerms = sharedState.graph->getNumNodes(tx, termsTableID);
    for (auto offset = 0u; offset < numTerms; ++offset) {
        if (!termsMask.isMasked(offset)) {
            continue;
        }
        termNodeID.offset = offset;
        frontierPair->getNextFrontierUnsafe().ptrCast<PathLengths>()->setActive(termNodeID);
    }
}

void runFrontiersOnce(processor::ExecutionContext* executionContext, QFTSState& qFtsState,
    graph::Graph* graph, common::idx_t tfPropertyIdx) {
    auto frontierPair = qFtsState.frontierPair.get();
    frontierPair->beginNewIteration();
    auto relTableIDInfos = graph->getRelTableIDInfos();
    auto& appearsInTableInfo = relTableIDInfos[0];
    frontierPair->beginFrontierComputeBetweenTables(appearsInTableInfo.fromNodeTableID,
        appearsInTableInfo.toNodeTableID);
    GDSUtils::scheduleFrontierTask(appearsInTableInfo.relTableID, graph, ExtendDirection::FWD,
        qFtsState, executionContext, 1 /* numThreads */, tfPropertyIdx);
}

class QFTSOutputWriter {
public:
    QFTSOutputWriter(storage::MemoryManager* mm, QFTSOutput* qFTSOutput,
        const QFTSGDSBindData& bindData);

    void write(processor::FactorizedTable& scoreFT, nodeID_t docNodeID, uint64_t len);

    std::unique_ptr<QFTSOutputWriter> copy();

private:
    QFTSOutput* qFTSOutput;
    common::ValueVector termsVector;
    common::ValueVector docsVector;
    common::ValueVector scoreVector;
    std::vector<common::ValueVector*> vectors;
    common::idx_t pos;
    storage::MemoryManager* mm;
    const QFTSGDSBindData& bindData;
};

QFTSOutputWriter::QFTSOutputWriter(storage::MemoryManager* mm, QFTSOutput* qFTSOutput,
    const QFTSGDSBindData& bindData)
    : qFTSOutput{std::move(qFTSOutput)}, termsVector{LogicalType::INTERNAL_ID(), mm},
      docsVector{LogicalType::INTERNAL_ID(), mm}, scoreVector{LogicalType::UINT64(), mm}, mm{mm},
      bindData{bindData} {
    auto state = DataChunkState::getSingleValueDataChunkState();
    pos = state->getSelVector()[0];
    termsVector.setState(state);
    docsVector.setState(state);
    scoreVector.setState(state);
    vectors.push_back(&termsVector);
    vectors.push_back(&docsVector);
    vectors.push_back(&scoreVector);
}

void QFTSOutputWriter::write(processor::FactorizedTable& scoreFT, nodeID_t docNodeID,
    uint64_t len) {
    bool hasScore = qFTSOutput->scores.contains(docNodeID);
    termsVector.setNull(pos, !hasScore);
    docsVector.setNull(pos, !hasScore);
    scoreVector.setNull(pos, !hasScore);
    // See comments in FTSBindData for the meaning of k and b.
    auto k = bindData.k;
    auto b = bindData.b;
    if (hasScore) {
        auto scoreInfo = qFTSOutput->scores.at(docNodeID);
        double score = 0;
        for (auto& scoreData : scoreInfo.scoreData) {
            auto numDocs = bindData.numDocs;
            auto avgDocLen = bindData.avgDocLen;
            auto df = scoreData.df;
            auto tf = scoreData.tf;
            score += log10((numDocs - df + 0.5) / (df + 0.5) + 1) *
                     ((tf * (k + 1) / (tf + k * (1 - b + b * (len / avgDocLen)))));
        }
        termsVector.setValue(pos, scoreInfo.termID);
        docsVector.setValue(pos, docNodeID);
        scoreVector.setValue(pos, score);
    }
    scoreFT.append(vectors);
}

std::unique_ptr<QFTSOutputWriter> QFTSOutputWriter::copy() {
    return std::make_unique<QFTSOutputWriter>(mm, qFTSOutput, bindData);
}

class QFTSVertexCompute : public VertexCompute {
public:
    explicit QFTSVertexCompute(storage::MemoryManager* mm,
        processor::GDSCallSharedState* sharedState, std::unique_ptr<QFTSOutputWriter> writer)
        : mm{mm}, sharedState{sharedState}, writer{std::move(writer)} {
        scoreFT = sharedState->claimLocalTable(mm);
    }

    ~QFTSVertexCompute() override { sharedState->returnLocalTable(scoreFT); }

    void vertexCompute(const graph::VertexScanState::Chunk& chunk) override;

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<QFTSVertexCompute>(mm, sharedState, writer->copy());
    }

private:
    storage::MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    processor::FactorizedTable* scoreFT;
    std::unique_ptr<QFTSOutputWriter> writer;
};

void QFTSVertexCompute::vertexCompute(const graph::VertexScanState::Chunk& chunk) {
    auto docLens = chunk.getProperties<uint64_t>(0);
    for (auto i = 0u; i < chunk.getNodeIDs().size(); i++) {
        writer->write(*scoreFT, chunk.getNodeIDs()[i], docLens[i]);
    }
}

void runVertexComputeIteration(processor::ExecutionContext* executionContext, graph::Graph* graph,
    VertexCompute& vc) {
    auto maxThreads = executionContext->clientContext->getCurrentSetting(main::ThreadsSetting::name)
                          .getValue<uint64_t>();
    auto sharedState = std::make_shared<VertexComputeTaskSharedState>(maxThreads, graph);
    auto docsTableID = graph->getNodeTableIDs()[1];
    auto info = VertexComputeTaskInfo(vc, {QFTSAlgorithm::DOC_LEN_PROP_NAME});
    GDSUtils::runVertexComputeOnTable(docsTableID, graph, sharedState, info, *executionContext);
}

void QFTSAlgorithm::exec(processor::ExecutionContext* executionContext) {
    auto termsTableID = sharedState->graph->getNodeTableIDs()[0];
    KU_ASSERT(sharedState->getInputNodeMaskMap()->containsTableID(termsTableID));
    auto termsMask = sharedState->getInputNodeMaskMap()->getOffsetMask(termsTableID);
    auto output = std::make_unique<QFTSOutput>();

    // Do edge compute to extend terms -> docs and save the term frequency and document frequency
    // for each term-doc pair. The reason why we store the term frequency and document frequency
    // is that: we need the `len` property from the docs table which is only available during the
    // vertex compute.
    auto numNodes = sharedState->graph->getNumNodesMap(executionContext->clientContext->getTx());
    auto currentFrontier = std::make_shared<PathLengths>(numNodes,
        executionContext->clientContext->getMemoryManager());
    auto nextFrontier = std::make_shared<PathLengths>(numNodes,
        executionContext->clientContext->getMemoryManager());
    auto frontierPair = std::make_unique<DoublePathLengthsFrontierPair>(currentFrontier,
        nextFrontier, 1 /* numThreads */);
    auto edgeCompute = std::make_unique<QFTSEdgeCompute>(frontierPair.get(), &output->scores,
        sharedState->nodeProp);
    QFTSState qFTSState = QFTSState{std::move(frontierPair), std::move(edgeCompute), termsTableID};
    qFTSState.initFirstFrontierWithTerms(*sharedState, *termsMask,
        executionContext->clientContext->getTx(), termsTableID);

    runFrontiersOnce(executionContext, qFTSState, sharedState->graph.get(),
        executionContext->clientContext->getCatalog()
            ->getTableCatalogEntry(executionContext->clientContext->getTx(),
                sharedState->graph->getRelTableIDs()[0])
            ->getPropertyIdx(QFTSAlgorithm::TERM_FREQUENCY_PROP_NAME));

    // Do vertex compute to calculate the score for doc with the length property.
    QFTSOutputWriter outputWriter{executionContext->clientContext->getMemoryManager(), output.get(),
        *bindData->ptrCast<QFTSGDSBindData>()};
    auto writerVC =
        std::make_unique<QFTSVertexCompute>(executionContext->clientContext->getMemoryManager(),
            sharedState.get(), outputWriter.copy());
    runVertexComputeIteration(executionContext, sharedState->graph.get(), *writerVC);
    sharedState->mergeLocalTables();
}

static std::shared_ptr<Expression> getScoreColumn(Binder* binder) {
    return binder->createVariable(QFTSAlgorithm::SCORE_PROP_NAME, LogicalType::DOUBLE());
}

binder::expression_vector QFTSAlgorithm::getResultColumns(binder::Binder* binder) const {
    expression_vector columns;
    auto& termsNode = bindData->getNodeInput()->constCast<NodeExpression>();
    columns.push_back(termsNode.getInternalID());
    auto& docsNode = bindData->getNodeOutput()->constCast<NodeExpression>();
    columns.push_back(docsNode.getInternalID());
    columns.push_back(getScoreColumn(binder));
    return columns;
}

void QFTSAlgorithm::bind(const binder::expression_vector& params, binder::Binder* binder,
    graph::GraphEntry& graphEntry) {
    KU_ASSERT(params.size() == 6);
    auto termNode = params[1];
    auto k = ExpressionUtil::getLiteralValue<double>(*params[2]);
    auto b = ExpressionUtil::getLiteralValue<double>(*params[3]);
    auto numDocs = ExpressionUtil::getLiteralValue<uint64_t>(*params[4]);
    auto avgDocLen = ExpressionUtil::getLiteralValue<double>(*params[5]);
    auto nodeOutput = bindNodeOutput(binder, graphEntry);
    bindData = std::make_unique<QFTSGDSBindData>(termNode, nodeOutput, k, b, numDocs, avgDocLen);
}

function::function_set QFTSFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<QFTSAlgorithm>();
    result.push_back(
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo)));
    return result;
}

} // namespace fts_extension
} // namespace kuzu
