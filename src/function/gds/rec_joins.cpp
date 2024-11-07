#include "function/gds/rec_joins.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/exception/interrupt.h"
#include "common/exception/runtime.h"
#include "common/task_system/progress_bar.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_utils.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/local_storage/local_node_table.h"
#include "storage/local_storage/local_storage.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

RJBindData::RJBindData(const RJBindData& other) : GDSBindData{other} {
    nodeInput = other.nodeInput;
    lowerBound = other.lowerBound;
    upperBound = other.upperBound;
    semantic = other.semantic;
    extendDirection = other.extendDirection;
    flipPath = other.flipPath;
    writePath = other.writePath;
    directionExpr = other.directionExpr;
    lengthExpr = other.lengthExpr;
    pathNodeIDsExpr = other.pathNodeIDsExpr;
    pathEdgeIDsExpr = other.pathEdgeIDsExpr;
}

PathsOutputWriterInfo RJBindData::getPathWriterInfo() const {
    auto info = PathsOutputWriterInfo();
    info.semantic = semantic;
    info.lowerBound = lowerBound;
    info.flipPath = flipPath;
    info.writeEdgeDirection = writePath && extendDirection == ExtendDirection::BOTH;
    info.writePath = writePath;
    return info;
}

void RJAlgorithm::setToNoPath() {
    bindData->ptrCast<RJBindData>()->writePath = false;
}

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

binder::expression_vector RJAlgorithm::getResultColumnsNoPath() {
    expression_vector columns;
    auto& inputNode = bindData->getNodeInput()->constCast<NodeExpression>();
    columns.push_back(inputNode.getInternalID());
    auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
    columns.push_back(outputNode.getInternalID());
    columns.push_back(bindData->ptrCast<RJBindData>()->lengthExpr);
    return columns;
}

expression_vector RJAlgorithm::getBaseResultColumns() const {
    expression_vector columns;
    auto& inputNode = bindData->getNodeInput()->constCast<NodeExpression>();
    columns.push_back(inputNode.getInternalID());
    auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
    columns.push_back(outputNode.getInternalID());
    auto rjBindData = bindData->ptrCast<RJBindData>();
    if (rjBindData->extendDirection == ExtendDirection::BOTH) {
        columns.push_back(rjBindData->directionExpr);
    }
    columns.push_back(rjBindData->lengthExpr);
    return columns;
}

void RJAlgorithm::bindColumnExpressions(binder::Binder* binder) const {
    auto rjBindData = bindData->ptrCast<RJBindData>();
    if (rjBindData->extendDirection == common::ExtendDirection::BOTH) {
        rjBindData->directionExpr =
            binder->createVariable(DIRECTION_COLUMN_NAME, LogicalType::LIST(LogicalType::BOOL()));
    }
    rjBindData->lengthExpr = binder->createVariable(LENGTH_COLUMN_NAME, LogicalType::UINT16());
    rjBindData->pathNodeIDsExpr = binder->createVariable(PATH_NODE_IDS_COLUMN_NAME,
        LogicalType::LIST(LogicalType::INTERNAL_ID()));
    rjBindData->pathEdgeIDsExpr = binder->createVariable(PATH_EDGE_IDS_COLUMN_NAME,
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
    auto extendDirection =
        ExtendDirectionUtil::fromString(ExpressionUtil::getLiteralValue<std::string>(*params[3]));
    bindData = std::make_unique<RJBindData>(nodeInput, nodeOutput, lowerBound, upperBound,
        PathSemantic::WALK, extendDirection);
    bindColumnExpressions(binder);
}

// All recursive join computation have the same vertex compute. This vertex compute writes
// result (could be dst, length or path) from a dst node ID to given source node ID.
class RJVertexCompute : public VertexCompute {
public:
    RJVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        std::unique_ptr<RJOutputWriter> writer)
        : mm{mm}, sharedState{sharedState}, writer{std::move(writer)} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~RJVertexCompute() override { sharedState->returnLocalTable(localFT); }

    bool beginOnTable(table_id_t tableID) override {
        if (!sharedState->inNbrTableIDs(tableID)) {
            return false;
        }
        writer->beginWritingForDstNodesInTable(tableID);
        return true;
    }

    void vertexCompute(nodeID_t nodeID) override {
        if (sharedState->exceedLimit() || writer->skip(nodeID)) {
            return;
        }
        writer->write(*localFT, nodeID, sharedState->counter.get());
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<RJVertexCompute>(mm, sharedState, writer->copy());
    }

private:
    storage::MemoryManager* mm;
    // Shared state storing ftables to materialize output.
    processor::GDSCallSharedState* sharedState;
    processor::FactorizedTable* localFT;
    std::unique_ptr<RJOutputWriter> writer;
};

static double getRJProgress(common::offset_t totalNumNodes, common::offset_t completedNumNodes) {
    if (totalNumNodes == 0) {
        return 0;
    }
    return (double)completedNumNodes / totalNumNodes;
}

void RJAlgorithm::exec(processor::ExecutionContext* executionContext) {
    auto clientContext = executionContext->clientContext;
    auto graph = sharedState->graph.get();
    auto inputNodeMaskMap = sharedState->getInputNodeMaskMap();
    common::offset_t totalNumNodes = 0;
    if (inputNodeMaskMap->enabled()) {
        totalNumNodes = inputNodeMaskMap->getNumMaskedNode();
    } else {
        for (auto& tableID : graph->getNodeTableIDs()) {
            totalNumNodes += graph->getNumNodes(executionContext->clientContext->getTx(), tableID);
        }
    }
    common::offset_t completedNumNodes = 0;
    for (auto& tableID : graph->getNodeTableIDs()) {
        if (!inputNodeMaskMap->containsTableID(tableID)) {
            continue;
        }
        auto calcFunc = [tableID, graph, executionContext, clientContext, this](offset_t offset) {
            if (clientContext->interrupted()) {
                throw InterruptException{};
            }
            auto sourceNodeID = nodeID_t{offset, tableID};
            RJCompState rjCompState = getRJCompState(executionContext, sourceNodeID);
            rjCompState.initSource(sourceNodeID);
            auto rjBindData = bindData->ptrCast<RJBindData>();
            GDSUtils::runFrontiersUntilConvergence(executionContext, rjCompState, graph,
                rjBindData->extendDirection, rjBindData->upperBound);
            auto vertexCompute =
                std::make_unique<RJVertexCompute>(clientContext->getMemoryManager(),
                    sharedState.get(), rjCompState.outputWriter->copy());
            GDSUtils::runVertexComputeIteration(executionContext, graph, *vertexCompute);
        };
        auto numNodes = graph->getNumNodes(executionContext->clientContext->getTx(), tableID);
        auto mask = inputNodeMaskMap->getOffsetMask(tableID);
        if (mask->isEnabled()) {
            for (const auto& offset : mask->range(0, numNodes)) {
                calcFunc(offset);
                clientContext->getProgressBar()->updateProgress(executionContext->queryID,
                    getRJProgress(totalNumNodes, completedNumNodes++));
                if (sharedState->exceedLimit()) {
                    break;
                }
            }
        } else {
            for (auto offset = 0u; offset < numNodes; ++offset) {
                calcFunc(offset);
                clientContext->getProgressBar()->updateProgress(executionContext->queryID,
                    getRJProgress(totalNumNodes, completedNumNodes++));
                if (sharedState->exceedLimit()) {
                    break;
                }
            }
        }
    }
    sharedState->mergeLocalTables();
}

} // namespace function
} // namespace kuzu
