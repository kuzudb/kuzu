#include "function/gds/rec_joins.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/exception/runtime.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_utils.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"
#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

RJCompState::RJCompState(std::unique_ptr<function::FrontierPair> frontierPair,
    std::unique_ptr<function::EdgeCompute> edgeCompute, std::unique_ptr<RJOutputs> outputs,
    std::unique_ptr<RJOutputWriter> outputWriter)
    : frontierPair{std::move(frontierPair)}, edgeCompute{std::move(edgeCompute)},
      outputs{std::move(outputs)}, outputWriter{std::move(outputWriter)} {}

void RJAlgorithm::validateLowerUpperBound(int64_t lowerBound, int64_t upperBound) {
    if (lowerBound < 0 || upperBound < 0) {
        throw RuntimeException(
            stringFormat("Lower and upper bound lengths of recursive join operations need to be "
                         "non-negative. Given lower bound is: {} and upper bound is: {}.",
                lowerBound, upperBound));
    }
    if (lowerBound > upperBound) {
        throw RuntimeException(
            stringFormat("Lower bound length of recursive join operations need to be less than or "
                         "equal to upper bound. Given lower bound is: {} and upper bound is: {}.",
                lowerBound, upperBound));
    }
    if (upperBound >= RJBindData::DEFAULT_MAXIMUM_ALLOWED_UPPER_BOUND) {
        throw RuntimeException(
            stringFormat("Recursive join operations only works for non-positive upper bound "
                         "iterations that are up to {}. Given upper bound is: {}.",
                RJBindData::DEFAULT_MAXIMUM_ALLOWED_UPPER_BOUND, upperBound));
    }
}

expression_vector RJAlgorithm::getNodeIDResultColumns() const {
    expression_vector columns;
    auto& inputNode = bindData->getNodeInput()->constCast<NodeExpression>();
    columns.push_back(inputNode.getInternalID());
    auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
    columns.push_back(outputNode.getInternalID());
    return columns;
}

std::shared_ptr<Expression> RJAlgorithm::getLengthColumn(Binder* binder) const {
    return binder->createVariable(LENGTH_COLUMN_NAME, LogicalType::INT64());
}

std::shared_ptr<Expression> RJAlgorithm::getPathNodeIDsColumn(Binder* binder) const {
    return binder->createVariable(PATH_NODE_IDS_COLUMN_NAME,
        LogicalType::LIST(LogicalType::INTERNAL_ID()));
}

std::shared_ptr<Expression> RJAlgorithm::getPathEdgeIDsColumn(Binder* binder) const {
    return binder->createVariable(PATH_EDGE_IDS_COLUMN_NAME,
        LogicalType::LIST(LogicalType::INTERNAL_ID()));
}

static void validateSPUpperBound(int64_t upperBound) {
    if (upperBound == 0) {
        throw RuntimeException(stringFormat("Shortest path operations only works for positive "
                                            "upper bound iterations. Given upper bound is: {}.",
            upperBound));
    }
}

void SPAlgorithm::bind(const expression_vector& params, Binder* binder,
    graph::GraphEntry& graphEntry) {
    KU_ASSERT(params.size() == 4);
    auto nodeInput = params[1];
    auto nodeOutput = bindNodeOutput(binder, graphEntry);
    auto lowerBound = 1;
    auto upperBound = ExpressionUtil::getLiteralValue<int64_t>(*params[2]);
    validateSPUpperBound(upperBound);
    validateLowerUpperBound(lowerBound, upperBound);
    auto outputProperty = ExpressionUtil::getLiteralValue<bool>(*params[3]);
    bindData =
        std::make_unique<RJBindData>(nodeInput, nodeOutput, outputProperty, lowerBound, upperBound);
}

class RJOutputWriterVCSharedState {
public:
    RJOutputWriterVCSharedState(storage::MemoryManager* mm, processor::FactorizedTable* globalFT,
        RJOutputWriter* rjOutputWriter)
        : mm{mm}, globalFT{globalFT}, rjOutputWriter{rjOutputWriter} {}

    std::mutex mtx;
    storage::MemoryManager* mm;
    processor::FactorizedTable* globalFT;
    RJOutputWriter* rjOutputWriter;
};

class RJOutputWriterVC : public VertexCompute {
public:
    explicit RJOutputWriterVC(RJOutputWriterVCSharedState* sharedState) : sharedState{sharedState} {
        localFT = std::make_unique<processor::FactorizedTable>(sharedState->mm,
            sharedState->globalFT->getTableSchema()->copy());
        localRJOutputWriter = sharedState->rjOutputWriter->copy();
    }

    void beginOnTable(table_id_t tableID) override {
        localRJOutputWriter->beginWritingForDstNodesInTable(tableID);
    }

    void vertexCompute(nodeID_t nodeID) override {
        if (localRJOutputWriter->skipWriting(nodeID)) {
            return;
        }
        localRJOutputWriter->write(*localFT, nodeID);
    }

    void finalizeWorkerThread() override {
        std::unique_lock lck(sharedState->mtx);
        sharedState->globalFT->merge(*localFT);
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<RJOutputWriterVC>(sharedState);
    }

private:
    RJOutputWriterVCSharedState* sharedState;
    std::unique_ptr<processor::FactorizedTable> localFT;
    std::unique_ptr<RJOutputWriter> localRJOutputWriter;
};

void RJAlgorithm::exec(processor::ExecutionContext* executionContext) {
    for (auto& tableID : sharedState->graph->getNodeTableIDs()) {
        if (!sharedState->inputNodeOffsetMasks.contains(tableID)) {
            continue;
        }
        auto mask = sharedState->inputNodeOffsetMasks.at(tableID).get();
        for (auto offset = 0u; offset < sharedState->graph->getNumNodes(tableID); ++offset) {
            if (!mask->isMasked(offset, offset)) {
                continue;
            }
            auto sourceNodeID = nodeID_t{offset, tableID};
            RJCompState rjCompState = getRJCompState(executionContext, sourceNodeID);
            rjCompState.initRJFromSource(sourceNodeID);
            GDSUtils::runFrontiersUntilConvergence(executionContext, rjCompState,
                sharedState->graph.get(), bindData->ptrCast<RJBindData>()->upperBound);
            auto writerVCSharedState = std::make_unique<RJOutputWriterVCSharedState>(
                executionContext->clientContext->getMemoryManager(), sharedState->fTable.get(),
                rjCompState.outputWriter.get());
            auto writerVC = std::make_unique<RJOutputWriterVC>(writerVCSharedState.get());
            GDSUtils::runVertexComputeIteration(executionContext, sharedState->graph.get(),
                *writerVC);
        }
    }
}

} // namespace function
} // namespace kuzu
