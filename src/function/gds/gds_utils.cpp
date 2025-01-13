#include "function/gds/gds_utils.h"

#include "catalog/catalog.h"
#include "common/task_system/task_scheduler.h"
#include "function/gds/gds_task.h"
#include "graph/graph.h"
#include "graph/graph_entry.h"
#include "main/settings.h"

using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::processor;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

GDSComputeState::GDSComputeState(std::unique_ptr<function::FrontierPair> frontierPair,
    std::unique_ptr<function::EdgeCompute> edgeCompute,
    processor::NodeOffsetMaskMap* outputNodeMask)
    : frontierPair{std::move(frontierPair)}, edgeCompute{std::move(edgeCompute)},
      outputNodeMask{outputNodeMask} {}

GDSComputeState::~GDSComputeState() = default;

void GDSComputeState::beginFrontierComputeBetweenTables(common::table_id_t currTableID,
    common::table_id_t nextTableID) {
    frontierPair->beginFrontierComputeBetweenTables(currTableID, nextTableID);
}

static uint64_t getNumThreads(processor::ExecutionContext& context) {
    return context.clientContext->getCurrentSetting(main::ThreadsSetting::name)
        .getValue<uint64_t>();
}

void GDSUtils::scheduleFrontierTask(catalog::TableCatalogEntry* fromEntry,
    catalog::TableCatalogEntry* toEntry, catalog::TableCatalogEntry* relEntry, graph::Graph* graph,
    ExtendDirection extendDirection, GDSComputeState& gdsComputeState,
    processor::ExecutionContext* context, std::optional<uint64_t> numThreads,
    const std::string& propertyToScan) {
    auto clientContext = context->clientContext;
    auto transaction = clientContext->getTransaction();
    auto info = FrontierTaskInfo(fromEntry, toEntry, relEntry, graph, extendDirection,
        *gdsComputeState.edgeCompute, propertyToScan);
    auto maxThreads = numThreads ? numThreads.value() : getNumThreads(*context);
    auto sharedState =
        std::make_shared<FrontierTaskSharedState>(maxThreads, *gdsComputeState.frontierPair);
    auto task = std::make_shared<FrontierTask>(maxThreads, info, sharedState);

    if (gdsComputeState.frontierPair->isCurFrontierSparse()) {
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
    auto numNodes = graph->getNumNodes(transaction, fromEntry->getTableID());
    sharedState->morselDispatcher.init(numNodes);
    clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task, context,
        true /* launchNewWorkerThread */);
}

void GDSUtils::runFrontiersUntilConvergence(processor::ExecutionContext* context,
    GDSComputeState& compState, graph::Graph* graph, ExtendDirection extendDirection,
    uint64_t maxIters) {
    auto frontierPair = compState.frontierPair.get();
    compState.edgeCompute->resetSingleThreadState();
    while (frontierPair->continueNextIter(maxIters)) {
        frontierPair->beginNewIteration();
        if (compState.outputNodeMask != nullptr && compState.outputNodeMask->enabled() &&
            compState.edgeCompute->terminate(*compState.outputNodeMask)) {
            break;
        }
        for (auto& [fromEntry, toEntry, relEntry] : graph->getRelFromToEntryInfos()) {
            switch (extendDirection) {
            case ExtendDirection::FWD: {
                compState.beginFrontierComputeBetweenTables(fromEntry->getTableID(),
                    toEntry->getTableID());
                scheduleFrontierTask(fromEntry, toEntry, relEntry, graph, ExtendDirection::FWD,
                    compState, context);
            } break;
            case ExtendDirection::BWD: {
                compState.beginFrontierComputeBetweenTables(toEntry->getTableID(),
                    fromEntry->getTableID());
                scheduleFrontierTask(toEntry, fromEntry, relEntry, graph, ExtendDirection::BWD,
                    compState, context);
            } break;
            case ExtendDirection::BOTH: {
                compState.beginFrontierComputeBetweenTables(fromEntry->getTableID(),
                    toEntry->getTableID());
                scheduleFrontierTask(fromEntry, toEntry, relEntry, graph, ExtendDirection::FWD,
                    compState, context);
                compState.beginFrontierComputeBetweenTables(toEntry->getTableID(),
                    fromEntry->getTableID());
                scheduleFrontierTask(toEntry, fromEntry, relEntry, graph, ExtendDirection::BWD,
                    compState, context);
            } break;
            default:
                KU_UNREACHABLE;
            }
        }
    }
}

static void runVertexComputeInternal(catalog::TableCatalogEntry* currentEntry, graph::Graph* graph,
    std::shared_ptr<VertexComputeTask> task, processor::ExecutionContext* context) {
    auto numNodes =
        graph->getNumNodes(context->clientContext->getTransaction(), currentEntry->getTableID());
    auto sharedState = task->getSharedState();
    sharedState->morselDispatcher.init(numNodes);
    context->clientContext->getTaskScheduler()->scheduleTaskAndWaitOrError(task, context,
        true /* launchNewWorkerThread */);
}

void GDSUtils::runVertexCompute(processor::ExecutionContext* context, graph::Graph* graph,
    VertexCompute& vc, std::vector<std::string> propertiesToScan) {
    auto maxThreads = getNumThreads(*context);
    auto sharedState = std::make_shared<VertexComputeTaskSharedState>(maxThreads);
    for (auto& entry : graph->getGraphEntry()->nodeEntries) {
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
    auto maxThreads = getNumThreads(*context);
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
    std::vector<std::string> propertiesToScan;
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
