#include "function/fts.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/exception/runtime.h"
#include "common/types/internal_id_util.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_task.h"
#include "function/gds/gds_utils.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"
#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace fts_extension {

using namespace function;

struct FTSEdgeCompute : public EdgeCompute {
    DoublePathLengthsFrontierPair* frontierPair;
    common::node_id_map_t<std::pair<nodeID_t, uint64_t>>* score;

    FTSEdgeCompute(DoublePathLengthsFrontierPair* frontierPair,
        common::node_id_map_t<std::pair<nodeID_t, uint64_t>>* score)
        : frontierPair{frontierPair}, score{score} {}

    bool edgeCompute(nodeID_t boundNodeID, nodeID_t nbrNodeID, relID_t /*edgeID*/) override {
        auto subScore = 5;
        if (!score->contains(nbrNodeID)) {
            score->emplace(nbrNodeID, std::make_pair(boundNodeID, subScore));
            return true;
        }

        score->at(nbrNodeID).second += subScore;
        return true;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<FTSEdgeCompute>(frontierPair, score);
    }
};

struct FTSOutput {
public:
    explicit FTSOutput() = default;
    virtual ~FTSOutput() = default;

    common::node_id_map_t<std::pair<common::nodeID_t, uint64_t>> score;
};

void runFrontiersOnce(processor::ExecutionContext* executionContext, FTSState& ftsState,
    graph::Graph* graph) {
    auto frontierPair = ftsState.frontierPair.get();
    auto fc = ftsState.edgeCompute.get();
    frontierPair->beginNewIteration();
    for (auto& relTableIDInfo : graph->getRelTableIDInfos()) {
        frontierPair->beginFrontierComputeBetweenTables(relTableIDInfo.fromNodeTableID,
            relTableIDInfo.toNodeTableID);
        auto sharedState = std::make_shared<FrontierTaskSharedState>(*frontierPair, graph, *fc,
            relTableIDInfo.relTableID);
        GDSUtils::parallelizeFrontierCompute(executionContext, sharedState);
    }
}

void FTSAlgorithm::exec(processor::ExecutionContext* executionContext) {
    auto tableID = sharedState->graph->getNodeTableIDs()[0];
    if (!sharedState->inputNodeOffsetMasks.contains(tableID)) {
        return;
    }
    auto mask = sharedState->inputNodeOffsetMasks.at(tableID).get();
    auto output = std::make_unique<FTSOutput>();
    for (auto offset = 0u; offset < sharedState->graph->getNumNodes(tableID); ++offset) {
        if (!mask->isMasked(offset, offset)) {
            continue;
        }
        auto sourceNodeID = nodeID_t{offset, tableID};
        auto frontierPair = std::make_unique<DoublePathLengthsFrontierPair>(
            sharedState->graph->getNodeTableIDAndNumNodes(),
            executionContext->clientContext->getMaxNumThreadForExec(),
            executionContext->clientContext->getMemoryManager());
        auto edgeCompute = std::make_unique<FTSEdgeCompute>(frontierPair.get(), &output->score);
        FTSState ftsState = FTSState{std::move(frontierPair), std::move(edgeCompute)};
        ftsState.initFTSFromSource(sourceNodeID);
        runFrontiersOnce(executionContext, ftsState, sharedState->graph.get());
    }

    auto table = sharedState->fTable;
    auto state = DataChunkState::getSingleValueDataChunkState();
    common::ValueVector srcNodeIDVector = common::ValueVector{LogicalType::INTERNAL_ID(),
        executionContext->clientContext->getMemoryManager()};
    common::ValueVector dstNodeIDVector = common::ValueVector{LogicalType::INTERNAL_ID(),
        executionContext->clientContext->getMemoryManager()};
    common::ValueVector scoreVector = common::ValueVector{LogicalType::UINT64(),
        executionContext->clientContext->getMemoryManager()};
    srcNodeIDVector.setState(state);
    dstNodeIDVector.setState(state);
    scoreVector.setState(state);
    std::vector<common::ValueVector*> vectors;
    vectors.push_back(&srcNodeIDVector);
    vectors.push_back(&dstNodeIDVector);
    vectors.push_back(&scoreVector);
    for (auto& [id, score] : output->score) {
        dstNodeIDVector.setValue(0, id);
        srcNodeIDVector.setValue(0, score.first);
        scoreVector.setValue(0, score.second);
        table->append(vectors);
    }
    auto d = 5;
}

static std::shared_ptr<Expression> getScoreColumn(Binder* binder) {
    return binder->createVariable(FTSAlgorithm::SCORE_COLUMN_NAME, LogicalType::UINT64());
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
    auto queryString = params[2]->constCast<LiteralExpression>().getValue().toString();
    auto outputProperty = params[3]->constCast<LiteralExpression>().getValue().getValue<bool>();
    bindData = std::make_unique<FTSBindData>(nodeInput, queryString, nodeOutput, outputProperty);
}

function::function_set FTSFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(name, std::make_unique<FTSAlgorithm>()));
    return result;
}

} // namespace fts_extension
} // namespace kuzu
