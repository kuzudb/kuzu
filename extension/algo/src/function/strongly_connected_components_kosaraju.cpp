#include "binder/binder.h"
#include "common/exception/interrupt.h"
#include "common/exception/runtime.h"
#include "common/task_system/progress_bar.h"
#include "function/algo_function.h"
#include "function/config/connected_components_config.h"
#include "function/gds/gds_utils.h"
#include "function/gds/gds_vertex_compute.h"
#include "processor/execution_context.h"

using namespace std;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;
using namespace kuzu::function;

namespace kuzu {
namespace algo_extension {

// Use the three largest offset_t values as special markers to avoid allocating another array.
// PROCESSED implies node has been VISITED and added to the backward DFS stack.
static constexpr offset_t PROCESSED = numeric_limits<offset_t>::max();
static constexpr offset_t VISITED = numeric_limits<offset_t>::max() - 1;
static constexpr offset_t NOT_VISITED = numeric_limits<offset_t>::max() - 2;

class KosarajuVisitedState {
public:
    KosarajuVisitedState(const offset_t tableID, const offset_t numNodes, MemoryManager* mm) {
        denseObjects.allocate(tableID, numNodes, mm);
        componentIDs = denseObjects.getData(tableID);
        for (auto i = 0u; i < numNodes; ++i) {
            componentIDs[i] = NOT_VISITED;
        }
    }

    // Visited meaning an offset has been visited in DFS traversal
    void setVisited(const offset_t offset) const { componentIDs[offset] = VISITED; }
    bool visited(const offset_t offset) const { return componentIDs[offset] >= VISITED; }

    // Processed meaning an offset has been pushed to KosarajuVisitedOrder
    void setProcessed(const offset_t offset) const { componentIDs[offset] = PROCESSED; }
    bool processed(const offset_t offset) const { return componentIDs[offset] == PROCESSED; }

    bool componentIDSet(const offset_t offset) const { return componentIDs[offset] < NOT_VISITED; }
    void setComponentID(const offset_t offset, const offset_t value) const {
        componentIDs[offset] = value;
    }
    offset_t getComponentID(const offset_t offset) const { return componentIDs[offset]; }

private:
    offset_t* componentIDs = nullptr;
    GDSDenseObjectManager<offset_t> denseObjects;
};

// Maintain a visited order of DFS traversal. The last seen element is the first element in this
// class, i.e. this class is a stack implementation
class KosarajuVisitedOrder {
public:
    KosarajuVisitedOrder(const offset_t tableID, const offset_t numNodes, MemoryManager* mm) {
        denseObjects.allocate(tableID, numNodes, mm);
        curData = denseObjects.getData(tableID);
    }

    void push(offset_t offset) {
        KU_ASSERT(curData);
        curData[size_++] = offset;
    }

    offset_t pop() {
        KU_ASSERT(curData && size_ > 0);
        size_--;
        return curData[size_];
    }

    uint64_t size() const { return size_; }

private:
    offset_t* curData = nullptr;
    uint64_t size_ = 0;
    GDSDenseObjectManager<offset_t> denseObjects;
};

class Kosaraju {
public:
    Kosaraju(Graph* graph, KosarajuVisitedState& visitedState, KosarajuVisitedOrder& visitedOrder)
        : graph{graph}, visitedState{visitedState}, visitedOrder{visitedOrder} {
        KU_ASSERT(graph->getNodeTableIDs().size() == 1);
        nodeTableID = graph->getNodeTableIDs()[0];
        auto nbrInfos = graph->getRelInfos(nodeTableID);
        KU_ASSERT(nbrInfos.size() == 1);
        auto nbrInfo = nbrInfos[0];
        KU_ASSERT(nbrInfo.srcTableID == nbrInfo.dstTableID);
        scanState = graph->prepareRelScan(*nbrInfo.relGroupEntry, nbrInfo.relTableID,
            nbrInfo.dstTableID, {});
    }

    void compute(const offset_t maxOffset, NodeOffsetMaskMap* map, ExecutionContext* context) {
        auto progressBar = context->clientContext->getProgressBar();
        std::vector<offset_t> toProcess;
        for (auto i = 0u; i < maxOffset; ++i) {
            if (context->clientContext->interrupted()) {
                throw InterruptException();
            }
            if (map && !map->valid(i)) { // Skip nodes not in mask
                continue;
            }
            if (visitedState.visited(i)) { // Skip visited nodes
                continue;
            }
            KU_ASSERT(toProcess.empty());
            toProcess.push_back(i);
            forwardDFS(toProcess);
            // Update fwd progress
            double fwdProgress = static_cast<double>(i) / maxOffset;
            progressBar->updateProgress(context->queryID, fwdProgress * 0.5);
        }
        while (visitedOrder.size() > 0) {
            if (context->clientContext->interrupted()) {
                throw InterruptException();
            }
            auto offset = visitedOrder.pop();
            if (!visitedState.componentIDSet(offset)) {
                KU_ASSERT(toProcess.empty());
                toProcess.push_back(offset);
                backwardsDFS(offset, toProcess);
                // Update bwd progress
                auto bwdProgress = 1.0 - (double)visitedOrder.size() / maxOffset;
                progressBar->updateProgress(context->queryID, 0.5 + bwdProgress * 0.5);
            }
        }
    }

    void forwardDFS(std::vector<offset_t>& toProcess) {
        while (!toProcess.empty()) {
            auto offset = toProcess.back();
            if (visitedState.visited(offset)) {
                toProcess.pop_back();
                if (!visitedState.processed(offset)) {
                    visitedOrder.push(offset);
                    visitedState.setProcessed(offset);
                }
                continue;
            }
            visitedState.setVisited(offset);
            auto nextNodeID = nodeID_t{offset, nodeTableID};
            for (auto chunk : graph->scanFwd(nextNodeID, *scanState)) {
                chunk.forEach([&](auto neighbors, auto, auto i) {
                    auto nbrNodeID = neighbors[i];
                    if (!visitedState.visited(nbrNodeID.offset)) {
                        toProcess.push_back(nbrNodeID.offset);
                    }
                });
            }
        }
    }

    void backwardsDFS(offset_t componentID, std::vector<offset_t>& toProcess) {
        while (!toProcess.empty()) {
            auto nextNode = toProcess.back();
            toProcess.pop_back();
            if (visitedState.componentIDSet(nextNode)) {
                continue;
            }
            visitedState.setComponentID(nextNode, componentID);
            auto nextNodeID = nodeID_t{nextNode, nodeTableID};
            for (auto chunk : graph->scanBwd(nextNodeID, *scanState)) {
                chunk.forEach([&](auto neighbors, auto, auto i) {
                    auto nbrNodeID = neighbors[i];
                    if (!visitedState.componentIDSet(nbrNodeID.offset)) {
                        toProcess.push_back(nbrNodeID.offset);
                    }
                });
            }
        }
    }

private:
    Graph* graph;
    table_id_t nodeTableID;
    std::unique_ptr<NbrScanState> scanState;
    KosarajuVisitedState& visitedState;
    KosarajuVisitedOrder& visitedOrder;
};

class KosarajuVertexCompute : public GDSResultVertexCompute {
public:
    KosarajuVertexCompute(MemoryManager* mm, GDSFuncSharedState* sharedState,
        KosarajuVisitedState& visitedState)
        : GDSResultVertexCompute{mm, sharedState}, visitedState{visitedState} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID());
        componentIDVector = createVector(LogicalType::UINT64());
    }

    void beginOnTableInternal(table_id_t /*tableID*/) override {}

    void vertexCompute(const offset_t startOffset, const offset_t endOffset,
        const table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (skip(i)) {
                continue;
            }
            auto nodeID = nodeID_t{i, tableID};
            nodeIDVector->setValue<nodeID_t>(0, nodeID);
            componentIDVector->setValue<uint64_t>(0, visitedState.getComponentID(i));
            localFT->append(vectors);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<KosarajuVertexCompute>(mm, sharedState, visitedState);
    }

private:
    KosarajuVisitedState& visitedState;
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> componentIDVector;
};

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    auto clientContext = input.context->clientContext;
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto mm = clientContext->getMemoryManager();
    auto graph = sharedState->graph.get();
    KU_ASSERT(graph->getNodeTableIDs().size() == 1);
    auto tableID = graph->getNodeTableIDs()[0];
    auto nodeMask = sharedState->getGraphNodeMaskMap();
    if (nodeMask) {
        nodeMask->pin(tableID);
    }
    auto maxOffset = graph->getMaxOffset(clientContext->getTransaction(), tableID);
    auto visitedState = KosarajuVisitedState(tableID, maxOffset, mm);
    auto visitedOrder = KosarajuVisitedOrder(tableID, maxOffset, mm);
    auto kosaraju = Kosaraju(graph, visitedState, visitedOrder);
    kosaraju.compute(maxOffset, nodeMask, input.context);

    auto vertexCompute = std::make_unique<KosarajuVertexCompute>(mm, sharedState, visitedState);
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vertexCompute);
    sharedState->factorizedTablePool.mergeLocalTables();
    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
    if (graphEntry.nodeInfos.size() != 1) {
        throw RuntimeException("Kosaraju's SCC only supports operations on one node table.");
    }
    if (graphEntry.relInfos.size() != 1) {
        throw RuntimeException("Kosaraju's SCC only supports operations on one edge table.");
    }
    expression_vector columns;
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries());
    columns.push_back(nodeOutput->constPtrCast<NodeExpression>()->getInternalID());
    columns.push_back(input->binder->createVariable(GROUP_ID_COLUMN_NAME, LogicalType::INT64()));
    return std::make_unique<GDSBindData>(std::move(columns), std::move(graphEntry), nodeOutput);
}

function_set SCCKosarajuFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::ANY});
    func->bindFunc = bindFunc;
    func->tableFunc = tableFunc;
    func->initSharedStateFunc = GDSFunction::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    func->getLogicalPlanFunc = GDSFunction::getLogicalPlan;
    func->getPhysicalPlanFunc = GDSFunction::getPhysicalPlan;
    result.push_back(std::move(func));
    return result;
}

} // namespace algo_extension
} // namespace kuzu
