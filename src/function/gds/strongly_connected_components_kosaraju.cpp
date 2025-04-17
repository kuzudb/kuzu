#include "binder/binder.h"
#include "common/exception/runtime.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_utils.h"
#include "gds_vertex_compute.h"
#include "processor/execution_context.h"

using namespace std;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

class KosarajuComponentIDs {
public:
    KosarajuComponentIDs(const offset_t tableID, const offset_t maxOffset, MemoryManager* mm) {
        denseObjects.allocate(tableID, maxOffset, mm);
        curData = denseObjects.getData(tableID);
        for (auto i = 0u; i < maxOffset; ++i) {
            curData[i] = INVALID_OFFSET;
        }
    }

    void pinTableID(table_id_t tableID) {
        curData = denseObjects.getData(tableID);
    }


    void setID(offset_t offset, offset_t componentID) {
        curData[offset] = componentID;
    }

    bool hasID(offset_t offset) const {
        return getID(offset) != INVALID_OFFSET;
    }
    offset_t getID(offset_t offset) const {
        return curData[offset];
    }

private:
    offset_t* curData = nullptr;
    GDSDenseObjectManager<offset_t> denseObjects;
};

class KosarajuDFSStack {
public:
    KosarajuDFSStack(const offset_t tableID, const offset_t maxOffset, MemoryManager* mm) {
        denseObjects.allocate(tableID, maxOffset, mm);
        curData = denseObjects.getData(tableID);
        for (auto i = 0u; i < maxOffset; ++i) {
            curData[i] = INVALID_OFFSET;
        }
    }

    void pinTableID(table_id_t tableID) {
        curData = denseObjects.getData(tableID);
    }

    void add(offset_t offset) {
        KU_ASSERT(curData);
        curData[counter++] = offset;
    }

    offset_t getOffset(offset_t offset) const {
        return curData[offset];
    }

private:
    offset_t counter = 0;
    offset_t* curData = nullptr;
    GDSDenseObjectManager<offset_t> denseObjects;
};

class KosarajuVisitedArray {
public:
    KosarajuVisitedArray(const offset_t tableID, const offset_t numNodes, MemoryManager* mm) {
        denseObjects.allocate(tableID, numNodes, mm);
        curData = denseObjects.getData(tableID);
        for (auto i = 0u; i < numNodes; ++i) {
            curData[i] = false;
        }
    }

    void pinTableID(table_id_t tableID) {
        curData = denseObjects.getData(tableID);
    }

    void visit(offset_t offset) {
        KU_ASSERT(!curData[offset]);
        curData[offset] = true;
    }

    bool visited(offset_t offset) {
        KU_ASSERT(curData);
        return curData[offset];
    }

private:
    bool* curData = nullptr;
    GDSDenseObjectManager<bool> denseObjects;
};

class Kosaraju {
public:
    Kosaraju(main::ClientContext* context, Graph* graph, table_id_t tableID,
        NbrScanState& scanState, KosarajuDFSStack& stack, KosarajuVisitedArray& visitedArray,
        KosarajuComponentIDs& componentIDs)
        : context{context}, graph {graph}, tableID{tableID}, scanState{scanState}, stack {stack},
        visitedArray {visitedArray}, componentIDs {componentIDs} {
        KU_ASSERT(graph->getNodeTableIDs().size() == 1 && graph->getRelTableIDs().size() == 1);
        nodeTableID = graph->getNodeTableIDs()[0];
        
    }

    void exec() {
        auto maxOffset = graph->getMaxOffset(context->getTransaction(), tableID);
        for (auto i = 0u; i < maxOffset; ++i) {
            if (visitedArray.visited(i)) {
                continue;
            }
            dfsForward(i);
        }
        for (auto i = 0u; i < maxOffset; ++i) {
            auto offset = stack.getOffset(i);
            if (componentIDs.hasID(offset)) {
                continue;
            }
            dfsBackward(offset, offset);
        }
    }

private:
    void dfsForward(offset_t offset) {
        visitedArray.visit(offset);
        auto nodeID = nodeID_t{offset, tableID};
        for (auto chunk :  graph->scanFwd(nodeID, scanState)) {
            chunk.forEach([&](auto nbrNodeID, auto) {
                if (!isVisited(nbrNodeID.offset)) {
                    dfsForward(nbrNodeID.offset);
                }
            });
        }
        stack.add(offset);
    }

    void dfsBackward(offset_t offset, offset_t componentID) {
        componentIDs.setID(offset, componentID);
        auto nodeID = nodeID_t{offset, tableID};
        for (auto chunk :  graph->scanBwd(nodeID, scanState)) {
            chunk.forEach([&](auto nbrNodeID, auto) {
                if (!componentIDs.hasID(nodeID.offset)) {
                    dfsBackward(offset, componentID);
                }
            });
        }
    }

private:
    main::ClientContext* context;
    Graph* graph;
    table_id_t nodeTableID;
    NbrScanState& scanState;
    KosarajuDFSStack& stack;
    KosarajuVisitedArray& visitedArray;
    KosarajuComponentIDs& componentIDs;
};

class SCCVertexCompute : public GDSResultVertexCompute {
public:
    SCCVertexCompute(MemoryManager* mm, GDSFuncSharedState* sharedState, KosarajuComponentIDs& componentIDs)
        : GDSResultVertexCompute{mm, sharedState}, componentIDs{componentIDs} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID());
        componentIDVector = createVector(LogicalType::UINT64());
    }

    void beginOnTableInternal(table_id_t /*tableID*/) override {}

    void vertexCompute(const offset_t startOffset, const offset_t endOffset,
        const table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            auto nodeID = nodeID_t{i, tableID};
            nodeIDVector->setValue<nodeID_t>(0, nodeID);
            componentIDVector->setValue<uint64_t>(0, componentIDs.getID(i));
            localFT->append(vectors);
        }
    }

    unique_ptr<VertexCompute> copy() override {
        return std::make_unique<SCCVertexCompute>(mm, sharedState, componentIDs);
    }

private:
    KosarajuComponentIDs& componentIDs;
    unique_ptr<ValueVector> nodeIDVector;
    unique_ptr<ValueVector> componentIDVector;
};

static common::offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    auto clientContext = input.context->clientContext;
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto mm = clientContext->getMemoryManager();
    auto graph = sharedState->graph.get();
    KU_ASSERT(graph->getNodeTableIDs().size() == 1);
    auto tableID = graph->getNodeTableIDs()[0];
    auto maxOffset = graph->getMaxOffset(clientContext->getTransaction(), tableID);

    auto componentIDs = KosarajuComponentIDs(tableID, maxOffset, mm);
    auto visitedArray = KosarajuVisitedArray(tableID, maxOffset, mm);
    auto stack = KosarajuDFSStack(tableID, maxOffset, mm);


    // auto sccState = SCCState(tableID, maxOffset, mm);
    // auto edgeCompute = make_unique<SCCCompute>(graph, sccState);
    // edgeCompute->compute(tableID, maxOffset);

    auto vertexCompute = make_unique<SCCVertexCompute>(mm, sharedState, sccState);
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vertexCompute);

    sharedState->factorizedTablePool.mergeLocalTables();
    return 0;
}

static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
    if (graphEntry.nodeInfos.size() != 1 || graphEntry.relInfos.size() != 1) {
        throw RuntimeException("Kosaraju's SCC only supports operations on one node table.");
    }
    // if (graphEntry.relInfos.size() != 1) {
    //     throw RuntimeException("Kosaraju's SCC only supports operations on one edge table.");
    // }
    expression_vector columns;
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries());
    columns.push_back(nodeOutput->constPtrCast<NodeExpression>()->getInternalID());
    columns.push_back(input->binder->createVariable(GROUP_ID_COLUMN_NAME, LogicalType::INT64()));
    return std::make_unique<GDSBindData>(std::move(columns), std::move(graphEntry), nodeOutput);
}

function_set SCCKosarajuFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(SCCKosarajuFunction::name,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY});
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

} // namespace function
} // namespace kuzu
