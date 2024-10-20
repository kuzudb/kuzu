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
#include "storage/local_storage/local_node_table.h"
#include "storage/local_storage/local_storage.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

PathsOutputWriterInfo RJBindData::getPathWriterInfo() const {
    auto info = PathsOutputWriterInfo();
    info.semantic = semantic;
    info.lowerBound = lowerBound;
    info.extendFromSource = extendFromSource;
    info.writeEdgeDirection = extendDirection == ExtendDirection::BOTH;
    return info;
}

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

expression_vector RJAlgorithm::getBaseResultColumns(Binder* binder) const {
    expression_vector columns;
    auto& inputNode = bindData->getNodeInput()->constCast<NodeExpression>();
    columns.push_back(inputNode.getInternalID());
    auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
    columns.push_back(outputNode.getInternalID());
    auto rjBindData = bindData->ptrCast<RJBindData>();
    if (rjBindData->extendDirection == ExtendDirection::BOTH) {
        if (rjBindData->directionExpr != nullptr) {
            columns.push_back(rjBindData->directionExpr);
        } else {
            columns.push_back(binder->createVariable(DIRECTION_COLUMN_NAME,
                LogicalType::LIST(LogicalType::BOOL())));
        }
    }
    return columns;
}

std::shared_ptr<Expression> RJAlgorithm::getLengthColumn(Binder* binder) const {
    if (bindData->ptrCast<RJBindData>()->lengthExpr != nullptr) {
        return bindData->ptrCast<RJBindData>()->lengthExpr;
    }
    return binder->createVariable(LENGTH_COLUMN_NAME, LogicalType::INT64());
}

std::shared_ptr<Expression> RJAlgorithm::getPathNodeIDsColumn(Binder* binder) const {
    if (bindData->ptrCast<RJBindData>()->pathNodeIDsExpr != nullptr) {
        return bindData->ptrCast<RJBindData>()->pathNodeIDsExpr;
    }
    return binder->createVariable(PATH_NODE_IDS_COLUMN_NAME,
        LogicalType::LIST(LogicalType::INTERNAL_ID()));
}

std::shared_ptr<Expression> RJAlgorithm::getPathEdgeIDsColumn(Binder* binder) const {
    if (bindData->ptrCast<RJBindData>()->pathEdgeIDsExpr != nullptr) {
        return bindData->ptrCast<RJBindData>()->pathEdgeIDsExpr;
    }
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
    auto extendDirection =
        ExtendDirectionUtil::fromString(ExpressionUtil::getLiteralValue<std::string>(*params[3]));
    bindData = std::make_unique<RJBindData>(nodeInput, nodeOutput, lowerBound, upperBound,
        PathSemantic::WALK, extendDirection);
}

// All recursive join computation have the same vertex compute. This vertex compute writes
// result (could be dst, length or path) from a dst node ID to given source node ID.
class RJVertexCompute : public VertexCompute {
public:
    explicit RJVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        std::unique_ptr<RJOutputWriter> writer)
        : mm{mm}, sharedState{sharedState}, writer{std::move(writer)} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~RJVertexCompute() override { sharedState->returnLocalTable(localFT); }

    void beginOnTable(table_id_t tableID) override {
        writer->beginWritingForDstNodesInTable(tableID);
    }

    void vertexCompute(nodeID_t nodeID) override {
        if (writer->skipWriting(nodeID)) {
            return;
        }
        writer->write(*localFT, nodeID);
    }

    void finalizeWorkerThread() override {
        // Do nothing.
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

void RJAlgorithm::exec(processor::ExecutionContext* executionContext) {
    auto clientContext = executionContext->clientContext;
    auto inputNodeMaskMap = sharedState->getInputNodeMaskMap();
    for (auto& tableID : sharedState->graph->getNodeTableIDs()) {
        if (!inputNodeMaskMap->containsTableID(tableID)) {
            continue;
        }
        auto localTable = clientContext->getTx()->getLocalStorage()->getLocalTable(tableID);
        if (localTable && localTable->cast<storage::LocalNodeTable>().getNumRows()) {
            throw RuntimeException("Current recursive algorithm does not work with uncommited "
                                   "data. Execute COMMIT or ROLLBACK first.");
        }
        auto mask = inputNodeMaskMap->getOffsetMask(tableID);
        for (auto offset = 0u; offset < sharedState->graph->getNumNodes(tableID); ++offset) {
            if (!mask->isMasked(offset)) {
                continue;
            }
            auto sourceNodeID = nodeID_t{offset, tableID};
            RJCompState rjCompState = getRJCompState(executionContext, sourceNodeID);
            rjCompState.initSource(sourceNodeID);
            auto rjBindData = bindData->ptrCast<RJBindData>();
            GDSUtils::runFrontiersUntilConvergence(executionContext, rjCompState,
                sharedState->graph.get(), rjBindData->extendDirection, rjBindData->upperBound);
            auto vertexCompute =
                std::make_unique<RJVertexCompute>(clientContext->getMemoryManager(),
                    sharedState.get(), rjCompState.outputWriter->copy());
            GDSUtils::runVertexComputeIteration(executionContext, sharedState->graph.get(),
                *vertexCompute);
        }
    }
    sharedState->mergeLocalTables();
}

} // namespace function
} // namespace kuzu
