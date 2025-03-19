#include "function/gds/gds_utils.h"

#include "binder/expression/property_expression.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/task_system/task_scheduler.h"
#include "function/gds/gds_task.h"
#include "graph/graph.h"
#include "graph/graph_entry.h"
#include <re2.h>

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::function;
using namespace kuzu::processor;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

static std::shared_ptr<FrontierTask> getFrontierTask(TableCatalogEntry* fromEntry,
    TableCatalogEntry* toEntry, TableCatalogEntry* relEntry, Graph* graph,
    ExtendDirection extendDirection, GDSComputeState& computeState, main::ClientContext* context,
    const std::string& propertyToScan) {
    computeState.beginFrontierCompute(fromEntry->getTableID(), toEntry->getTableID());
    auto info = FrontierTaskInfo(fromEntry, toEntry, relEntry, graph, extendDirection,
        *computeState.edgeCompute, propertyToScan);
    auto numThreads = context->getMaxNumThreadForExec();
    auto sharedState =
        std::make_shared<FrontierTaskSharedState>(numThreads, *computeState.frontierPair);
    auto maxOffset = graph->getMaxOffset(context->getTransaction(), fromEntry->getTableID());
    sharedState->morselDispatcher.init(maxOffset);
    return std::make_shared<FrontierTask>(numThreads, info, sharedState);
}

static void scheduleFrontierTask(TableCatalogEntry* fromEntry, TableCatalogEntry* toEntry,
    TableCatalogEntry* relEntry, Graph* graph, ExtendDirection extendDirection,
    GDSComputeState& computeState, ExecutionContext* context, const std::string& propertyToScan) {
    auto clientContext = context->clientContext;
    auto task = getFrontierTask(fromEntry, toEntry, relEntry, graph, extendDirection, computeState,
        clientContext, propertyToScan);
    if (computeState.frontierPair->isCurFrontierSparse()) {
        task->runSparse();
        return;
    }
    // GDSUtils::runFrontiersUntilConvergence is called from a GDSCall operator, which is
    // already executed by a worker thread Tm of the task scheduler. So this function is
    // executed by Tm. Because this function will monitor the task and wait for it to
    // complete, running GDS algorithms effectively "loses" Tm. This can even lead to the
    // query processor to halt, e.g., if there is a single worker thread in the system, and
    // more generally decrease the number of worker threads by 1. Therefore, we instruct
    // scheduleTaskAndWaitOrError to start a new thread by passing true as the last
    // argument.
    clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task, context,
        true /* launchNewWorkerThread */);
}

static void runOnGraph(ExecutionContext* context, Graph* graph, ExtendDirection extendDirection,
    GDSComputeState& compState, const std::string& propertyToScan) {
    for (auto info : graph->getGraphEntry()->nodeInfos) {
        auto fromEntry = info.entry;
        for (auto& nbrInfo : graph->getForwardNbrTableInfos(fromEntry->getTableID())) {
            auto toEntry = nbrInfo.nodeEntry;
            auto relEntry = nbrInfo.relEntry;
            switch (extendDirection) {
            case ExtendDirection::FWD: {
                scheduleFrontierTask(fromEntry, toEntry, relEntry, graph, ExtendDirection::FWD,
                    compState, context, propertyToScan);
            } break;
            case ExtendDirection::BWD: {
                scheduleFrontierTask(toEntry, fromEntry, relEntry, graph, ExtendDirection::BWD,
                    compState, context, propertyToScan);
            } break;
            case ExtendDirection::BOTH: {
                scheduleFrontierTask(fromEntry, toEntry, relEntry, graph, ExtendDirection::FWD,
                    compState, context, propertyToScan);
                scheduleFrontierTask(toEntry, fromEntry, relEntry, graph, ExtendDirection::BWD,
                    compState, context, propertyToScan);
            } break;
            default:
                KU_UNREACHABLE;
            }
        }
    }
}

void GDSUtils::runFrontiersUntilConvergence(ExecutionContext* context, GDSComputeState& compState,
    Graph* graph, ExtendDirection extendDirection, uint64_t maxIteration) {
    auto frontierPair = compState.frontierPair.get();
    while (frontierPair->continueNextIter(maxIteration)) {
        frontierPair->beginNewIteration();
        runOnGraph(context, graph, extendDirection, compState, "" /* empty */);
    }
}

void GDSUtils::runFrontiersUntilConvergence(ExecutionContext* context, GDSComputeState& compState,
    Graph* graph, ExtendDirection extendDirection, uint64_t maxIteration,
    common::NodeOffsetMaskMap* outputNodeMask, const std::string& propertyToScan) {
    auto frontierPair = compState.frontierPair.get();
    compState.edgeCompute->resetSingleThreadState();
    while (frontierPair->continueNextIter(maxIteration)) {
        frontierPair->beginNewIteration();
        if (outputNodeMask != nullptr && outputNodeMask->enabled() &&
            compState.edgeCompute->terminate(*outputNodeMask)) {
            break;
        }
        runOnGraph(context, graph, extendDirection, compState, propertyToScan);
    }
}

static void runVertexComputeInternal(catalog::TableCatalogEntry* currentEntry, graph::Graph* graph,
    std::shared_ptr<VertexComputeTask> task, processor::ExecutionContext* context) {
    auto maxOffset =
        graph->getMaxOffset(context->clientContext->getTransaction(), currentEntry->getTableID());
    auto sharedState = task->getSharedState();
    sharedState->morselDispatcher.init(maxOffset);
    context->clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task, context,
        true /* launchNewWorkerThread */);
}

void GDSUtils::runVertexCompute(processor::ExecutionContext* context, graph::Graph* graph,
    VertexCompute& vc, std::vector<std::string> propertiesToScan) {
    auto maxThreads = context->clientContext->getMaxNumThreadForExec();
    auto sharedState = std::make_shared<VertexComputeTaskSharedState>(maxThreads);
    for (auto& nodeInfo : graph->getGraphEntry()->nodeInfos) {
        auto entry = nodeInfo.entry;
        if (!vc.beginOnTable(entry->getTableID())) {
            continue;
        }
        auto info = VertexComputeTaskInfo(vc, graph, entry, propertiesToScan);
        auto task = std::make_shared<VertexComputeTask>(maxThreads, info, sharedState);
        runVertexComputeInternal(entry, graph, task, context);
    }
}

void GDSUtils::runVertexCompute(ExecutionContext* context, Graph* graph, VertexCompute& vc) {
    runVertexCompute(context, graph, vc, std::vector<std::string>{});
}

void GDSUtils::runVertexCompute(ExecutionContext* context, Graph* graph, VertexCompute& vc,
    catalog::TableCatalogEntry* entry, std::vector<std::string> propertiesToScan) {
    auto maxThreads = context->clientContext->getMaxNumThreadForExec();
    auto info = VertexComputeTaskInfo(vc, graph, entry, propertiesToScan);
    auto sharedState = std::make_shared<VertexComputeTaskSharedState>(maxThreads);
    if (!vc.beginOnTable(entry->getTableID())) {
        return;
    }
    auto task = std::make_shared<VertexComputeTask>(maxThreads, info, sharedState);
    runVertexComputeInternal(entry, graph, task, context);
}

void GDSUtils::runVertexComputeSparse(SparseFrontier& sparseFrontier, graph::Graph* graph,
    VertexCompute& vc) {
    for (auto& tableID : graph->getNodeTableIDs()) {
        if (!vc.beginOnTable(tableID)) {
            continue;
        }
        sparseFrontier.pinTableID(tableID);
        auto localVc = vc.copy();
        for (auto& offset : sparseFrontier.getOffsetSet()) {
            localVc->vertexCompute(offset, offset + 1, tableID);
        }
    }
}

} // namespace function
} // namespace kuzu
