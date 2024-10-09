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

struct EdgeProp {
    uint64_t df;
    uint64_t tf;

    EdgeProp(uint64_t df, uint64_t tf) : df{df}, tf{tf} {}
};

struct EdgeInfo {
    nodeID_t srcNode;
    std::vector<EdgeProp> edgeProps;

    explicit EdgeInfo(nodeID_t srcNode) : srcNode{std::move(srcNode)} {}

    void addEdge(uint64_t df, uint64_t tf) { edgeProps.push_back(EdgeProp{df, tf}); }
};

struct FTSEdgeCompute : public EdgeCompute {
    DoublePathLengthsFrontierPair* frontierPair;
    common::node_id_map_t<EdgeInfo>* score;
    common::node_id_map_t<uint64_t>* nodeProp;
    FTSEdgeCompute(DoublePathLengthsFrontierPair* frontierPair,
        common::node_id_map_t<EdgeInfo>* score, common::node_id_map_t<uint64_t>* nodeProp)
        : frontierPair{frontierPair}, score{score}, nodeProp{nodeProp} {}

    void edgeCompute(nodeID_t boundNodeID, std::span<const common::nodeID_t> nbrIDs,
        std::span<const relID_t>, SelectionVector& mask, bool /*isFwd*/,
        const ValueVector* edgeProp) override {
        KU_ASSERT(nodeProp->contains(boundNodeID));
        size_t activeCount = 0;
        mask.forEach([&](auto i) {
            auto nbrNodeID = nbrIDs[i];
            auto df = nodeProp->at(boundNodeID);
            auto tf = edgeProp->getValue<uint64_t>(i);
            if (!score->contains(nbrNodeID)) {
                score->emplace(nbrNodeID, EdgeInfo{boundNodeID});
            }
            score->at(nbrNodeID).addEdge(df, tf);
            mask.getMutableBuffer()[activeCount++] = i;
        });
        mask.setToFiltered(activeCount);
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<FTSEdgeCompute>(frontierPair, score, nodeProp);
    }
};

struct FTSOutput {
public:
    explicit FTSOutput() = default;
    virtual ~FTSOutput() = default;

    common::node_id_map_t<EdgeInfo> score;
};

void runFrontiersOnce(processor::ExecutionContext* executionContext, FTSState& ftsState,
    graph::Graph* graph, common::idx_t edgePropertyIndex) {
    auto frontierPair = ftsState.frontierPair.get();
    frontierPair->beginNewIteration();
    for (auto& relTableIDInfo : graph->getRelTableIDInfos()) {
        frontierPair->beginFrontierComputeBetweenTables(relTableIDInfo.fromNodeTableID,
            relTableIDInfo.toNodeTableID);
        auto sharedState = std::make_shared<FrontierTaskSharedState>(*frontierPair);
        auto clientContext = executionContext->clientContext;
        auto info = FrontierTaskInfo(relTableIDInfo.relTableID, graph, common::ExtendDirection::FWD,
            *ftsState.edgeCompute, edgePropertyIndex);
        auto maxThreads =
            clientContext->getCurrentSetting(main::ThreadsSetting::name).getValue<uint64_t>();
        auto task = std::make_shared<FrontierTask>(maxThreads, info, sharedState);
        // GDSUtils::runFrontiersUntilConvergence is called from a GDSCall operator, which is
        // already executed by a worker thread Tm of the task scheduler. So this function is
        // executed by Tm. Because this function will monitor the task and wait for it to
        // complete, running GDS algorithms effectively "loses" Tm. This can even lead to the
        // query processor to halt, e.g., if there is a single worker thread in the system, and
        // more generally decrease the number of worker threads by 1. Therefore, we instruct
        // scheduleTaskAndWaitOrError to start a new thread by passing true as the last
        // argument.
        clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task, executionContext,
            true /* launchNewWorkerThread */);
    }
}

class FTSOutputWriter {
public:
    FTSOutputWriter(storage::MemoryManager* mm, FTSOutput* ftsOutput, const FTSBindData& bindData)
        : ftsOutput{std::move(ftsOutput)}, srcNodeIDVector{LogicalType::INTERNAL_ID(), mm},
          dstNodeIDVector{LogicalType::INTERNAL_ID(), mm}, scoreVector{LogicalType::UINT64(), mm},
          mm{mm}, bindData{bindData} {
        auto state = DataChunkState::getSingleValueDataChunkState();
        pos = state->getSelVector()[0];
        srcNodeIDVector.setState(state);
        dstNodeIDVector.setState(state);
        scoreVector.setState(state);
        vectors.push_back(&srcNodeIDVector);
        vectors.push_back(&dstNodeIDVector);
        vectors.push_back(&scoreVector);
    }

    void write(processor::FactorizedTable& fTable, nodeID_t dstNodeID, uint64_t len) {
        bool hasScore = ftsOutput->score.contains(dstNodeID);
        srcNodeIDVector.setNull(pos, !hasScore);
        dstNodeIDVector.setNull(pos, !hasScore);
        scoreVector.setNull(pos, !hasScore);
        if (hasScore) {
            auto edgeInfo = ftsOutput->score.at(dstNodeID);
            double score = 0;
            for (auto& edgeProp : edgeInfo.edgeProps) {
                auto numDocs = bindData.numDocs;
                auto avgDL = bindData.avgDL;
                auto df = edgeProp.df;
                auto tf = edgeProp.tf;
                auto k = bindData.k;
                auto b = bindData.b;
                score += log10((numDocs - df + 0.5) / (df + 0.5) + 1) *
                         ((tf * (k + 1) / (tf + k * (1 - b + b * (len / avgDL)))));
            }
            srcNodeIDVector.setValue(pos, edgeInfo.srcNode);
            dstNodeIDVector.setValue(pos, dstNodeID);
            scoreVector.setValue(pos, score);
        }
        fTable.append(vectors);
    }

    std::unique_ptr<FTSOutputWriter> copy();

private:
    FTSOutput* ftsOutput;
    common::ValueVector srcNodeIDVector;
    common::ValueVector dstNodeIDVector;
    common::ValueVector scoreVector;
    std::vector<common::ValueVector*> vectors;
    common::idx_t pos;
    storage::MemoryManager* mm;
    const FTSBindData& bindData;
};

std::unique_ptr<FTSOutputWriter> FTSOutputWriter::copy() {
    return std::make_unique<FTSOutputWriter>(mm, ftsOutput, bindData);
}

class FTSOutputWriterSharedState {
public:
    FTSOutputWriterSharedState(storage::MemoryManager* mm, processor::FactorizedTable* globalFT,
        FTSOutputWriter* ftsOutputWriter)
        : mm{mm}, globalFT{globalFT}, ftsOutputWriter{ftsOutputWriter} {}

    std::mutex mtx;
    storage::MemoryManager* mm;
    processor::FactorizedTable* globalFT;
    FTSOutputWriter* ftsOutputWriter;
};

class FTSOutputWriterVC : public VertexCompute {
public:
    explicit FTSOutputWriterVC(FTSOutputWriterSharedState* sharedState) : sharedState{sharedState} {
        localFT = std::make_unique<processor::FactorizedTable>(sharedState->mm,
            sharedState->globalFT->getTableSchema()->copy());
        localFTSOutputWriter = sharedState->ftsOutputWriter->copy();
    }

    void beginOnTable(table_id_t /*tableID*/) override {}

    void vertexCompute(std::span<const nodeID_t> nodeIDs,
        std::span<const std::unique_ptr<ValueVector>> properties) override {
        for (auto i = 0u; i < nodeIDs.size(); i++) {
            localFTSOutputWriter->write(*localFT, nodeIDs[i], properties[0]->getValue<uint64_t>(i));
        }
    }

    void finalizeWorkerThread() override {
        std::unique_lock lck(sharedState->mtx);
        sharedState->globalFT->merge(*localFT);
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<FTSOutputWriterVC>(sharedState);
    }

private:
    FTSOutputWriterSharedState* sharedState;
    std::unique_ptr<processor::FactorizedTable> localFT;
    std::unique_ptr<FTSOutputWriter> localFTSOutputWriter;
};

void runVertexComputeIteration(processor::ExecutionContext* executionContext, graph::Graph* graph,
    VertexCompute& vc) {
    auto sharedState = std::make_shared<VertexComputeTaskSharedState>(graph, vc,
        executionContext->clientContext->getCurrentSetting(main::ThreadsSetting::name)
            .getValue<uint64_t>(),
        std::vector<std::string>{"len"});
    auto tableID = graph->getNodeTableIDs()[1];
    sharedState->morselDispatcher->init(tableID, graph->getNumNodes(tableID));
    auto task = std::make_shared<VertexComputeTask>(
        executionContext->clientContext->getCurrentSetting(main::ThreadsSetting::name)
            .getValue<uint64_t>(),
        sharedState);
    executionContext->clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task,
        executionContext, true /* launchNewWorkerThread */);
}

void FTSAlgorithm::exec(processor::ExecutionContext* executionContext) {
    auto tableID = sharedState->graph->getNodeTableIDs()[0];
    if (!sharedState->getInputNodeMaskMap()->containsTableID(tableID)) {
        return;
    }
    auto inputNodeMaskMap = sharedState->getInputNodeMaskMap();
    auto output = std::make_unique<FTSOutput>();
    auto mask = inputNodeMaskMap->getOffsetMask(tableID);
    for (auto offset = 0u; offset < sharedState->graph->getNumNodes(tableID); ++offset) {
        if (!mask->isMasked(offset)) {
            continue;
        }
        auto sourceNodeID = nodeID_t{offset, tableID};
        auto frontierPair = std::make_unique<DoublePathLengthsFrontierPair>(
            sharedState->graph->getNodeTableIDAndNumNodes(),
            executionContext->clientContext->getMaxNumThreadForExec(),
            executionContext->clientContext->getMemoryManager());
        auto edgeCompute = std::make_unique<FTSEdgeCompute>(frontierPair.get(), &output->score,
            sharedState->nodeProp);
        FTSState ftsState = FTSState{std::move(frontierPair), std::move(edgeCompute)};
        ftsState.initFTSFromSource(sourceNodeID);
        runFrontiersOnce(executionContext, ftsState, sharedState->graph.get(),
            executionContext->clientContext->getCatalog()
                ->getTableCatalogEntry(executionContext->clientContext->getTx(),
                    sharedState->graph->getRelTableIDs()[0])
                ->getPropertyIdx("tf"));
    }
    FTSOutputWriter outputWriter{executionContext->clientContext->getMemoryManager(), output.get(),
        *bindData->ptrCast<FTSBindData>()};
    auto ftsOutputWriterSharedState = std::make_unique<FTSOutputWriterSharedState>(
        executionContext->clientContext->getMemoryManager(), sharedState->fTable.get(),
        &outputWriter);
    auto writerVC = std::make_unique<FTSOutputWriterVC>(ftsOutputWriterSharedState.get());
    runVertexComputeIteration(executionContext, sharedState->graph.get(), *writerVC);
}

static std::shared_ptr<Expression> getScoreColumn(Binder* binder) {
    return binder->createVariable(FTSAlgorithm::SCORE_COLUMN_NAME, LogicalType::DOUBLE());
}

binder::expression_vector FTSAlgorithm::getResultColumns(binder::Binder* binder) const {
    expression_vector columns;
    auto& inputNode = bindData->getNodeInput()->constCast<NodeExpression>();
    columns.push_back(inputNode.getInternalID());
    auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
    columns.push_back(outputNode.getInternalID());
    columns.push_back(getScoreColumn(binder));
    return columns;
}

void FTSAlgorithm::bind(const binder::expression_vector& params, binder::Binder* binder,
    graph::GraphEntry& graphEntry) {
    KU_ASSERT(params.size() == 6);
    auto nodeInput = params[1];
    auto k = ExpressionUtil::getLiteralValue<double>(*params[2]);
    auto b = ExpressionUtil::getLiteralValue<double>(*params[3]);
    auto numDocs = ExpressionUtil::getLiteralValue<uint64_t>(*params[4]);
    auto avgDL = ExpressionUtil::getLiteralValue<double>(*params[5]);
    auto nodeOutput = bindNodeOutput(binder, graphEntry);
    bindData = std::make_unique<FTSBindData>(nodeInput, nodeOutput, k, b, numDocs, avgDL);
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
