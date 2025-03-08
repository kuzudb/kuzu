#include "function/gds/rec_joins.h"

#include "binder/binder.h"
#include "binder/expression/property_expression.h"
#include "common/exception/binder.h"
#include "common/exception/interrupt.h"
#include "common/task_system/progress_bar.h"
#include "function/gds/gds.h"
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
    weightPropertyExpr = other.weightPropertyExpr;
    weightOutputExpr = other.weightOutputExpr;
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

void RJAlgorithm::bind(const kuzu::function::GDSBindInput&, main::ClientContext&) {
    // LCOV_EXCL_START
    throw common::BinderException("Recursive join should not be triggered through function calls. "
                                  "Try cypher patter ()-[*]->() instead.");
    // LCOV_EXCL_STOP
}

void RJAlgorithm::setToNoPath() {
    bindData->ptrCast<RJBindData>()->writePath = false;
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
        writer->beginWritingOutputs(tableID);
        return true;
    }

    void vertexCompute(common::offset_t startOffset, common::offset_t endOffset,
        common::table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (sharedState->exceedLimit()) {
                return;
            }
            auto nodeID = nodeID_t{i, tableID};
            if (writer->skip(nodeID)) {
                continue;
            }
            writer->write(*localFT, nodeID, sharedState->counter.get());
        }
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

void RJAlgorithm::exec(processor::ExecutionContext* context) {
    auto clientContext = context->clientContext;
    auto graph = sharedState->graph.get();
    auto inputNodeMaskMap = sharedState->getInputNodeMaskMap();
    common::offset_t totalNumNodes = 0;
    if (inputNodeMaskMap->enabled()) {
        totalNumNodes = inputNodeMaskMap->getNumMaskedNode();
    } else {
        for (auto& tableID : graph->getNodeTableIDs()) {
            totalNumNodes += graph->getMaxOffset(clientContext->getTransaction(), tableID);
        }
    }
    common::offset_t completedNumNodes = 0;
    for (auto& tableID : graph->getNodeTableIDs()) {
        if (!inputNodeMaskMap->containsTableID(tableID)) {
            continue;
        }
        auto calcFunc = [tableID, graph, context, clientContext, this](offset_t offset) {
            if (clientContext->interrupted()) {
                throw InterruptException{};
            }
            auto sourceNodeID = nodeID_t{offset, tableID};
            auto rjCompState = getRJCompState(context, sourceNodeID);
            auto gdsComputeState = rjCompState.gdsComputeState.get();
            gdsComputeState->initSource(sourceNodeID);
            auto rjBindData = bindData->ptrCast<RJBindData>();
            std::string propertyName;
            if (rjBindData->weightPropertyExpr != nullptr) {
                propertyName = rjBindData->weightPropertyExpr->ptrCast<PropertyExpression>()
                                   ->getPropertyName();
            }
            GDSUtils::runFrontiersUntilConvergence(context, *gdsComputeState, graph,
                rjBindData->extendDirection, rjBindData->upperBound, propertyName);
            auto vertexCompute =
                std::make_unique<RJVertexCompute>(clientContext->getMemoryManager(),
                    sharedState.get(), rjCompState.outputWriter->copy());
            auto frontierPair = gdsComputeState->frontierPair.get();
            auto& candidates = frontierPair->getVertexComputeCandidates();
            candidates.mergeSparseFrontier(frontierPair->getNextSparseFrontier());
            if (candidates.enabled()) {
                GDSUtils::runVertexComputeSparse(candidates, graph, *vertexCompute);
            } else {
                GDSUtils::runVertexCompute(context, graph, *vertexCompute);
            }
        };
        auto maxOffset = graph->getMaxOffset(clientContext->getTransaction(), tableID);
        auto mask = inputNodeMaskMap->getOffsetMask(tableID);
        if (mask->isEnabled()) {
            for (const auto& offset : mask->range(0, maxOffset)) {
                calcFunc(offset);
                clientContext->getProgressBar()->updateProgress(context->queryID,
                    getRJProgress(totalNumNodes, completedNumNodes++));
                if (sharedState->exceedLimit()) {
                    break;
                }
            }
        } else {
            for (auto offset = 0u; offset < maxOffset; ++offset) {
                calcFunc(offset);
                clientContext->getProgressBar()->updateProgress(context->queryID,
                    getRJProgress(totalNumNodes, completedNumNodes++));
                if (sharedState->exceedLimit()) {
                    break;
                }
            }
        }
    }
    sharedState->mergeLocalTables();
}

std::unique_ptr<BFSGraph> RJAlgorithm::getBFSGraph(processor::ExecutionContext* context) {
    auto tx = context->clientContext->getTransaction();
    auto mm = context->clientContext->getMemoryManager();
    auto graph = sharedState->graph.get();
    auto bfsGraph = std::make_unique<BFSGraph>(graph->getMaxOffsetMap(tx), mm);
    auto vc = std::make_unique<BFSGraphInitVertexCompute>(*bfsGraph);
    GDSUtils::runVertexCompute(context, graph, *vc);
    return bfsGraph;
}

} // namespace function
} // namespace kuzu
