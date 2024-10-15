#include "function/fts.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
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
    common::node_id_map_t<uint64_t>* len;
    common::node_id_map_t<uint64_t>* nodeProp;
    FTSEdgeCompute(DoublePathLengthsFrontierPair* frontierPair,
        common::node_id_map_t<EdgeInfo>* score, common::node_id_map_t<uint64_t>* nodeProp)
        : frontierPair{frontierPair}, score{score}, nodeProp{nodeProp} {}

    bool edgeCompute(nodeID_t boundNodeID, nodeID_t nbrNodeID, relID_t /*edgeID*/, bool) override {
        KU_ASSERT(nodeProp->contains(boundNodeID));
        uint64_t df = nodeProp->at(boundNodeID);
        if (!score->contains(nbrNodeID)) {
            score->emplace(nbrNodeID, EdgeInfo{boundNodeID});
        }

        score->at(nbrNodeID).addEdge(df, df);
        return true;
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
    graph::Graph* graph) {
    auto frontierPair = ftsState.frontierPair.get();
    frontierPair->beginNewIteration();
    for (auto& relTableIDInfo : graph->getRelTableIDInfos()) {
        frontierPair->beginFrontierComputeBetweenTables(relTableIDInfo.fromNodeTableID,
            relTableIDInfo.toNodeTableID);
        auto sharedState = std::make_shared<FrontierTaskSharedState>(*frontierPair);
        auto clientContext = executionContext->clientContext;
        auto info = FrontierTaskInfo(relTableIDInfo.relTableID, graph, common::ExtendDirection::FWD,
            *ftsState.edgeCompute);
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
    FTSOutputWriter(storage::MemoryManager* mm, FTSOutput* ftsOutput)
        : ftsOutput{std::move(ftsOutput)}, srcNodeIDVector{LogicalType::INTERNAL_ID(), mm},
          dstNodeIDVector{LogicalType::INTERNAL_ID(), mm}, scoreVector{LogicalType::UINT64(), mm},
          mm{mm} {
        auto state = DataChunkState::getSingleValueDataChunkState();
        pos = state->getSelVector()[0];
        srcNodeIDVector.setState(state);
        dstNodeIDVector.setState(state);
        scoreVector.setState(state);
        vectors.push_back(&srcNodeIDVector);
        vectors.push_back(&dstNodeIDVector);
        vectors.push_back(&scoreVector);
    }

    void write(processor::FactorizedTable& fTable, nodeID_t dstNodeID) {
        bool hasScore = ftsOutput->score.contains(dstNodeID);
        srcNodeIDVector.setNull(pos, !hasScore);
        dstNodeIDVector.setNull(pos, !hasScore);
        scoreVector.setNull(pos, !hasScore);
        if (hasScore) {
            auto edgeInfo = ftsOutput->score.at(dstNodeID);
            double score = 0;
            for (auto& edgeProp : edgeInfo.edgeProps) {
                score += edgeProp.df;
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
};

std::unique_ptr<FTSOutputWriter> FTSOutputWriter::copy() {
    return std::make_unique<FTSOutputWriter>(mm, ftsOutput);
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

    void vertexCompute(nodeID_t nodeID) override { localFTSOutputWriter->write(*localFT, nodeID); }

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
            .getValue<uint64_t>());
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
    if (!sharedState->inputNodeOffsetMasks.contains(tableID)) {
        return;
    }
    auto mask = sharedState->inputNodeOffsetMasks.at(tableID).get();
    auto output = std::make_unique<FTSOutput>();
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
        runFrontiersOnce(executionContext, ftsState, sharedState->graph.get());
    }

    FTSOutputWriter outputWriter{executionContext->clientContext->getMemoryManager(), output.get()};
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
    auto nodeInput = params[1];
    auto nodeOutput = bindNodeOutput(binder, graphEntry);
    bindData = std::make_unique<FTSBindData>(nodeInput, nodeOutput);
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
