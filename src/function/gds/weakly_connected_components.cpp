#include "binder/binder.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_utils.h"
#include "gds_vertex_compute.h"
#include "processor/execution_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

struct OffsetManager {
    explicit OffsetManager(const table_id_map_t<offset_t>& maxOffsetMap) {
        std::vector<table_id_t> tableIDVector;
        for (auto [tableID, maxOffset] : maxOffsetMap) {
            tableIDVector.push_back(tableID);
        }
        std::sort(tableIDVector.begin(), tableIDVector.end());
        auto offset = 0u;
        for (auto tableID : tableIDVector) {
            tableIDToStartOffset.insert({tableID, offset});
            offset += maxOffsetMap.at(tableID);
        }
    }

    offset_t getStartOffset(table_id_t tableID) const { return tableIDToStartOffset.at(tableID); }

private:
    table_id_map_t<offset_t> tableIDToStartOffset;
};

class ComponentIDs {
public:
    ComponentIDs(const table_id_map_t<offset_t>& maxOffsetMap, const OffsetManager& offsetManager,
        MemoryManager* mm) {
        for (const auto& [tableID, maxOffset] : maxOffsetMap) {
            componentIDsMap.allocate(tableID, maxOffset, mm);
            pinTable(tableID);
            auto startOffset = offsetManager.getStartOffset(tableID);
            for (auto i = 0u; i < maxOffset; ++i) {
                toComponentIDs[i].store(i + startOffset, std::memory_order_relaxed);
            }
        }
    }

    void pinFromTable(table_id_t tableID) { fromComponentIDs = componentIDsMap.getData(tableID); }
    void pinToTable(table_id_t tableID) { toComponentIDs = componentIDsMap.getData(tableID); }
    void pinTable(table_id_t tableID) { toComponentIDs = componentIDsMap.getData(tableID); }

    bool update(offset_t boundOffset, offset_t nbrOffset) {
        auto boundValue = fromComponentIDs[boundOffset].load(std::memory_order_relaxed);
        auto tmp = toComponentIDs[nbrOffset].load(std::memory_order_relaxed);
        while (tmp > boundValue) {
            if (toComponentIDs[nbrOffset].compare_exchange_strong(tmp, boundValue)) {
                return true;
            }
        }
        return false;
    }

    offset_t getComponentID(offset_t offset) {
        return toComponentIDs[offset].load(std::memory_order_relaxed);
    }

private:
    std::atomic<offset_t>* fromComponentIDs = nullptr;
    std::atomic<offset_t>* toComponentIDs = nullptr;
    GDSDenseObjectManager<std::atomic<offset_t>> componentIDsMap;
};

class WCCAuxiliaryState : public GDSAuxiliaryState {
public:
    explicit WCCAuxiliaryState(ComponentIDs& componentIDs) : componentIDs{componentIDs} {}

    void beginFrontierCompute(table_id_t fromTableID, table_id_t toTableID) override {
        componentIDs.pinFromTable(fromTableID);
        componentIDs.pinToTable(toTableID);
    }

    bool update(offset_t boundOffset, offset_t nbrOffset) {
        return componentIDs.update(boundOffset, nbrOffset);
    }

    void switchToDense(ExecutionContext*, Graph*) override {}

private:
    ComponentIDs& componentIDs;
};

class WCCEdgeCompute : public EdgeCompute {
public:
    explicit WCCEdgeCompute(WCCAuxiliaryState& auxiliaryState) : auxiliaryState{auxiliaryState} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto nbrNodeID, auto) {
            if (auxiliaryState.update(boundNodeID.offset, nbrNodeID.offset)) {
                result.push_back(nbrNodeID);
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<WCCEdgeCompute>(auxiliaryState);
    }

private:
    WCCAuxiliaryState& auxiliaryState;
};

class WCCVertexCompute : public GDSResultVertexCompute {
public:
    WCCVertexCompute(storage::MemoryManager* mm, GDSFuncSharedState* sharedState,
        ComponentIDs& componentIDs)
        : GDSResultVertexCompute{mm, sharedState}, componentIDs{componentIDs} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID());
        componentIDVector = createVector(LogicalType::UINT64());
    }

    void beginOnTableInternal(table_id_t tableID) override { componentIDs.pinTable(tableID); }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (skip(i)) {
                continue;
            }
            auto nodeID = nodeID_t{i, tableID};
            nodeIDVector->setValue<nodeID_t>(0, nodeID);
            componentIDVector->setValue<uint64_t>(0, componentIDs.getComponentID(i));
            localFT->append(vectors);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WCCVertexCompute>(mm, sharedState, componentIDs);
    }

private:
    ComponentIDs& componentIDs;
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> componentIDVector;
};

static constexpr uint8_t MAX_ITERATION = 100;

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    auto clientContext = input.context->clientContext;
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto graph = sharedState->graph.get();
    auto currentFrontier = DenseFrontier::getUnvisitedFrontier(input.context, graph);
    auto nextFrontier =
        DenseFrontier::getVisitedFrontier(input.context, graph, sharedState->getGraphNodeMaskMap());
    auto frontierPair =
        std::make_unique<DenseFrontierPair>(std::move(currentFrontier), std::move(nextFrontier));
    frontierPair->setActiveNodesForNextIter();
    auto maxOffsetMap = graph->getMaxOffsetMap(clientContext->getTransaction());
    auto offsetManager = OffsetManager(maxOffsetMap);
    auto componentIDs =
        ComponentIDs(maxOffsetMap, offsetManager, clientContext->getMemoryManager());
    auto auxiliaryState = std::make_unique<WCCAuxiliaryState>(componentIDs);
    auto edgeCompute = std::make_unique<WCCEdgeCompute>(*auxiliaryState);
    auto vertexCompute = std::make_unique<WCCVertexCompute>(clientContext->getMemoryManager(),
        sharedState, componentIDs);
    auto computeState =
        GDSComputeState(std::move(frontierPair), std::move(edgeCompute), std::move(auxiliaryState));
    GDSUtils::runAlgorithmEdgeCompute(input.context, computeState, graph, ExtendDirection::BOTH,
        MAX_ITERATION);
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vertexCompute);
    sharedState->factorizedTablePool.mergeLocalTables();
    return 0;
}

static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries());
    expression_vector columns;
    columns.push_back(nodeOutput->constCast<NodeExpression>().getInternalID());
    columns.push_back(input->binder->createVariable(GROUP_ID_COLUMN_NAME, LogicalType::INT64()));
    return std::make_unique<GDSBindData>(std::move(columns), std::move(graphEntry), nodeOutput);
}

function_set WeaklyConnectedComponentsFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(WeaklyConnectedComponentsFunction::name,
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
