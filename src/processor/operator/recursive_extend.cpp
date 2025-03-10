#include "processor/operator/recursive_extend.h"

#include "binder/expression/property_expression.h"
#include "common/exception/interrupt.h"
#include "common/task_system/progress_bar.h"
#include "function/gds/compute.h"
#include "function/gds/gds_utils.h"
#include "processor/execution_context.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

// All recursive join computation have the same vertex compute. This vertex compute writes
// result (could be dst, length or path) from a dst node ID to given source node ID.
class RJVertexCompute : public VertexCompute {
public:
    RJVertexCompute(storage::MemoryManager* mm, RecursiveExtendSharedState* sharedState,
        std::unique_ptr<RJOutputWriter> writer)
        : mm{mm}, sharedState{sharedState}, writer{std::move(writer)} {
        localFT = sharedState->factorizedTablePool.claimLocalTable(mm);
    }
    ~RJVertexCompute() override { sharedState->factorizedTablePool.returnLocalTable(localFT); }

    bool beginOnTable(table_id_t tableID) override {
        if (!sharedState->inNbrTableIDs(tableID)) {
            return false;
        }
        writer->beginWritingOutputs(tableID);
        return true;
    }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t tableID) override {
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
    RecursiveExtendSharedState* sharedState;
    FactorizedTable* localFT;
    std::unique_ptr<RJOutputWriter> writer;
};

static double getRJProgress(offset_t totalNumNodes, offset_t completedNumNodes) {
    if (totalNumNodes == 0) {
        return 0;
    }
    return (double)completedNumNodes / totalNumNodes;
}

void RecursiveExtend::executeInternal(ExecutionContext* context) {
    auto clientContext = context->clientContext;
    auto graph = sharedState->graph.get();
    auto inputNodeMaskMap = sharedState->getInputNodeMaskMap();
    offset_t totalNumNodes = 0;
    if (inputNodeMaskMap->enabled()) {
        totalNumNodes = inputNodeMaskMap->getNumMaskedNode();
    } else {
        for (auto& tableID : graph->getNodeTableIDs()) {
            totalNumNodes += graph->getMaxOffset(clientContext->getTransaction(), tableID);
        }
    }
    offset_t completedNumNodes = 0;
    for (auto& tableID : graph->getNodeTableIDs()) {
        if (!inputNodeMaskMap->containsTableID(tableID)) {
            continue;
        }
        auto calcFunc = [tableID, graph, context, clientContext, this](offset_t offset) {
            if (clientContext->interrupted()) {
                throw InterruptException{};
            }
            auto sourceNodeID = nodeID_t{offset, tableID};
            auto rjCompState =
                function->getRJCompState(context, sourceNodeID, bindData, sharedState.get());
            auto gdsComputeState = rjCompState.gdsComputeState.get();
            gdsComputeState->initSource(sourceNodeID);
            std::string propertyName;
            if (bindData.weightPropertyExpr != nullptr) {
                propertyName =
                    bindData.weightPropertyExpr->ptrCast<PropertyExpression>()->getPropertyName();
            }
            GDSUtils::runFrontiersUntilConvergence(context, *gdsComputeState, graph,
                bindData.extendDirection, bindData.upperBound, propertyName);
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
    sharedState->factorizedTablePool.mergeLocalTables();
}

} // namespace processor
} // namespace kuzu
