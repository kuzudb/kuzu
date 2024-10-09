#include "function/fts.h"

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

struct FTSGDSBindData final : public function::GDSBindData {
    std::shared_ptr<binder::Expression> terms;
    // k: parameter controls the influence of term frequency saturation. It limits the effect of
    // additional occurrences of a term within a document.
    double_t k;
    // b: parameter controls the degree of length normalization by adjusting the influence of
    // document length.
    double_t b;
    uint64_t numDocs;
    double_t avgDocLen;

    FTSGDSBindData(std::shared_ptr<binder::Expression> terms,
        std::shared_ptr<binder::Expression> docs, double_t k, double_t b, uint64_t numDocs,
        double_t avgDocLen)
        : GDSBindData{std::move(docs)}, terms{std::move(terms)}, k{k}, b{b}, numDocs{numDocs},
          avgDocLen{avgDocLen} {}
    FTSGDSBindData(const FTSGDSBindData& other)
        : GDSBindData{other}, terms{other.terms}, k{other.k}, b{other.b}, numDocs{other.numDocs},
          avgDocLen{other.avgDocLen} {}

    bool hasNodeInput() const override { return true; }
    std::shared_ptr<binder::Expression> getNodeInput() const override { return terms; }

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<FTSGDSBindData>(*this);
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

struct FTSEdgeCompute : public EdgeCompute {
    DoublePathLengthsFrontierPair* termsFrontier;
    common::node_id_map_t<ScoreInfo>* scores;
    common::node_id_map_t<uint64_t>* dfs;

    FTSEdgeCompute(DoublePathLengthsFrontierPair* termsFrontier,
        common::node_id_map_t<ScoreInfo>* scores, common::node_id_map_t<uint64_t>* dfs)
        : termsFrontier{termsFrontier}, scores{scores}, dfs{dfs} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& resultChunk,
        bool) override;

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<FTSEdgeCompute>(termsFrontier, scores, dfs);
    }
};

std::vector<nodeID_t> FTSEdgeCompute::edgeCompute(nodeID_t boundNodeID,
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

struct FTSOutput {
    common::node_id_map_t<ScoreInfo> scores;

    FTSOutput() = default;
    virtual ~FTSOutput() = default;
};

struct FTSState : public function::GDSComputeState {
    void initTerms(processor::GDSCallSharedState& sharedState, RoaringBitmapSemiMask& termsMask,
        transaction::Transaction* tx, common::table_id_t termsTableID) const;

    FTSState(std::unique_ptr<function::FrontierPair> frontierPair,
        std::unique_ptr<function::EdgeCompute> edgeCompute, common::table_id_t termsTableID);
};

FTSState::FTSState(std::unique_ptr<function::FrontierPair> frontierPair,
    std::unique_ptr<function::EdgeCompute> edgeCompute, common::table_id_t termsTableID)
    : function::GDSComputeState{std::move(frontierPair), std::move(edgeCompute)} {
    this->frontierPair->getNextFrontierUnsafe().ptrCast<PathLengths>()->fixNextFrontierNodeTable(
        termsTableID);
}

void FTSState::initTerms(processor::GDSCallSharedState& sharedState,
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

void runFrontiersOnce(processor::ExecutionContext* executionContext, FTSState& ftsState,
    graph::Graph* graph, common::idx_t tfPropertyIdx) {
    auto frontierPair = ftsState.frontierPair.get();
    frontierPair->beginNewIteration();
    auto relTableIDInfos = graph->getRelTableIDInfos();
    auto& appearsInTableInfo = relTableIDInfos[0];
    frontierPair->beginFrontierComputeBetweenTables(appearsInTableInfo.fromNodeTableID,
        appearsInTableInfo.toNodeTableID);
    GDSUtils::scheduleFrontierTask(appearsInTableInfo.relTableID, graph, ExtendDirection::FWD,
        ftsState, executionContext, FTSAlgorithm::NUM_THREADS_FOR_EXECUTION, tfPropertyIdx);
}

class FTSOutputWriter {
public:
    FTSOutputWriter(storage::MemoryManager* mm, FTSOutput* ftsOutput,
        const FTSGDSBindData& bindData);

    void write(processor::FactorizedTable& scoreFT, nodeID_t docNodeID, uint64_t len);

    std::unique_ptr<FTSOutputWriter> copy();

private:
    FTSOutput* ftsOutput;
    common::ValueVector termsVector;
    common::ValueVector docsVector;
    common::ValueVector scoreVector;
    std::vector<common::ValueVector*> vectors;
    common::idx_t pos;
    storage::MemoryManager* mm;
    const FTSGDSBindData& bindData;
};

FTSOutputWriter::FTSOutputWriter(storage::MemoryManager* mm, FTSOutput* ftsOutput,
    const FTSGDSBindData& bindData)
    : ftsOutput{std::move(ftsOutput)}, termsVector{LogicalType::INTERNAL_ID(), mm},
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

void FTSOutputWriter::write(processor::FactorizedTable& scoreFT, nodeID_t docNodeID, uint64_t len) {
    bool hasScore = ftsOutput->scores.contains(docNodeID);
    termsVector.setNull(pos, !hasScore);
    docsVector.setNull(pos, !hasScore);
    scoreVector.setNull(pos, !hasScore);
    // See comments in FTSBindData for the meaning of k and b.
    auto k = bindData.k;
    auto b = bindData.b;
    if (hasScore) {
        auto scoreInfo = ftsOutput->scores.at(docNodeID);
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

std::unique_ptr<FTSOutputWriter> FTSOutputWriter::copy() {
    return std::make_unique<FTSOutputWriter>(mm, ftsOutput, bindData);
}

class FTSVertexCompute : public VertexCompute {
public:
    explicit FTSVertexCompute(storage::MemoryManager* mm,
        processor::GDSCallSharedState* sharedState, std::unique_ptr<FTSOutputWriter> writer)
        : mm{mm}, sharedState{sharedState}, writer{std::move(writer)} {
        scoreFT = sharedState->claimLocalTable(mm);
    }

    ~FTSVertexCompute() override { sharedState->returnLocalTable(scoreFT); }

    void vertexCompute(const graph::VertexScanState::Chunk& chunk) override;

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<FTSVertexCompute>(mm, sharedState, writer->copy());
    }

private:
    storage::MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    processor::FactorizedTable* scoreFT;
    std::unique_ptr<FTSOutputWriter> writer;
};

void FTSVertexCompute::vertexCompute(const graph::VertexScanState::Chunk& chunk) {
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
    auto info = VertexComputeTaskInfo(vc, {FTSAlgorithm::DOC_LEN_PROP_NAME});
    GDSUtils::runVertexComputeOnTable(docsTableID, graph, sharedState, info, *executionContext);
}

void FTSAlgorithm::exec(processor::ExecutionContext* executionContext) {
    auto termsTableID = sharedState->graph->getNodeTableIDs()[0];
    KU_ASSERT(sharedState->getInputNodeMaskMap()->containsTableID(termsTableID));
    auto inputNodeMasks = sharedState->getInputNodeMaskMap();
    auto termsMask = inputNodeMasks->getOffsetMask(termsTableID);
    auto output = std::make_unique<FTSOutput>();

    // Do edge compute to extend terms -> docs and save the term frequency and document frequency
    // for each term-doc pair. The reason why we store the term frequency and document frequency
    // is that: we need the `len` property from the docs table which is only available during the
    // vertex compute.
    auto frontierPair = std::make_unique<DoublePathLengthsFrontierPair>(
        sharedState->graph->getNumNodesMap(executionContext->clientContext->getTx()),
        FTSAlgorithm::NUM_THREADS_FOR_EXECUTION,
        executionContext->clientContext->getMemoryManager());
    auto edgeCompute = std::make_unique<FTSEdgeCompute>(frontierPair.get(), &output->scores,
        sharedState->nodeProp);
    FTSState ftsState = FTSState{std::move(frontierPair), std::move(edgeCompute), termsTableID};
    ftsState.initTerms(*sharedState, *termsMask, executionContext->clientContext->getTx(),
        termsTableID);

    runFrontiersOnce(executionContext, ftsState, sharedState->graph.get(),
        executionContext->clientContext->getCatalog()
            ->getTableCatalogEntry(executionContext->clientContext->getTx(),
                sharedState->graph->getRelTableIDs()[0])
            ->getPropertyIdx(FTSAlgorithm::TERM_FREQUENCY_PROP_NAME));

    // Do vertex compute to calculate the score for doc with the length property.
    FTSOutputWriter outputWriter{executionContext->clientContext->getMemoryManager(), output.get(),
        *bindData->ptrCast<FTSGDSBindData>()};
    auto writerVC =
        std::make_unique<FTSVertexCompute>(executionContext->clientContext->getMemoryManager(),
            sharedState.get(), outputWriter.copy());
    runVertexComputeIteration(executionContext, sharedState->graph.get(), *writerVC);
    sharedState->mergeLocalTables();
}

static std::shared_ptr<Expression> getScoreColumn(Binder* binder) {
    return binder->createVariable(FTSAlgorithm::SCORE_PROP_NAME, LogicalType::DOUBLE());
}

binder::expression_vector FTSAlgorithm::getResultColumns(binder::Binder* binder) const {
    expression_vector columns;
    auto& termsNode = bindData->getNodeInput()->constCast<NodeExpression>();
    columns.push_back(termsNode.getInternalID());
    auto& docsNode = bindData->getNodeOutput()->constCast<NodeExpression>();
    columns.push_back(docsNode.getInternalID());
    columns.push_back(getScoreColumn(binder));
    return columns;
}

void FTSAlgorithm::bind(const binder::expression_vector& params, binder::Binder* binder,
    graph::GraphEntry& graphEntry) {
    KU_ASSERT(params.size() == 6);
    auto termNode = params[1];
    auto k = ExpressionUtil::getLiteralValue<double>(*params[2]);
    auto b = ExpressionUtil::getLiteralValue<double>(*params[3]);
    auto numDocs = ExpressionUtil::getLiteralValue<uint64_t>(*params[4]);
    auto avgDocLen = ExpressionUtil::getLiteralValue<double>(*params[5]);
    auto nodeOutput = bindNodeOutput(binder, graphEntry);
    bindData = std::make_unique<FTSGDSBindData>(termNode, nodeOutput, k, b, numDocs, avgDocLen);
}

function::function_set FTSFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<FTSAlgorithm>();
    result.push_back(
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo)));
    return result;
}

} // namespace fts_extension
} // namespace kuzu
