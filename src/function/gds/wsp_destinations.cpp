#include "binder/expression/node_expression.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "processor/execution_context.h"
#include "wsp_utils.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

class Costs {
public:
    Costs(const table_id_map_t<offset_t>& maxOffsetMap, MemoryManager* mm) {
        init(maxOffsetMap, mm);
    }

    void pinTable(table_id_t tableID) { costs = costsMap.getData(tableID); }

    double getCost(offset_t offset) { return costs[offset].load(std::memory_order_relaxed); }

    void setCost(offset_t offset, double cost) {
        costs[offset].store(cost, std::memory_order_relaxed);
    }

    // CAS update nbrOffset if new path from boundOffset has a smaller cost.
    bool update(offset_t boundOffset, offset_t nbrOffset, double val) {
        auto newCost = getCost(boundOffset) + val;
        auto tmp = getCost(nbrOffset);
        while (newCost < tmp) {
            if (costs[nbrOffset].compare_exchange_strong(tmp, newCost)) {
                return true;
            }
        }
        return false;
    }

private:
    void init(const table_id_map_t<offset_t>& maxOffsetMap, MemoryManager* mm) {
        for (const auto& [tableID, maxOffset] : maxOffsetMap) {
            costsMap.allocate(tableID, maxOffset, mm);
            pinTable(tableID);
            for (auto i = 0u; i < maxOffset; ++i) {
                costs[i].store(std::numeric_limits<double>::max(), std::memory_order_relaxed);
            }
        }
    }

private:
    std::atomic<double>* costs = nullptr;
    GDSDenseObjectManager<std::atomic<double>> costsMap;
};

template<typename T>
class WSPDestinationsEdgeCompute : public EdgeCompute {
public:
    explicit WSPDestinationsEdgeCompute(std::shared_ptr<Costs> costs) : costs{std::move(costs)} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
            auto nbrNodeID = neighbors[i];
            auto weight = propertyVectors[0]->template getValue<T>(i);
            checkWeight(weight);
            if (costs->update(boundNodeID.offset, nbrNodeID.offset, static_cast<double>(weight))) {
                result.push_back(nbrNodeID);
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<WSPDestinationsEdgeCompute<T>>(costs);
    }

private:
    std::shared_ptr<Costs> costs;
};

class WSPDestinationsAuxiliaryState : public GDSAuxiliaryState {
public:
    explicit WSPDestinationsAuxiliaryState(std::shared_ptr<Costs> costs)
        : costs{std::move(costs)} {}

    Costs* getCosts() { return costs.get(); }

    void initSource(nodeID_t sourceNodeID) override { costs->setCost(sourceNodeID.offset, 0); }

    void beginFrontierCompute(table_id_t, table_id_t toTableID) override {
        costs->pinTable(toTableID);
    }

    void switchToDense(ExecutionContext*, graph::Graph*) override {}

private:
    std::shared_ptr<Costs> costs;
};

class WSPDestinationsOutputWriter : public RJOutputWriter {
public:
    WSPDestinationsOutputWriter(main::ClientContext* context, NodeOffsetMaskMap* outputNodeMask,
        nodeID_t sourceNodeID, Costs* costs, const table_id_map_t<offset_t>& maxOffsetMap)
        : RJOutputWriter{context, outputNodeMask, sourceNodeID}, costs{costs},
          maxOffsetMap{maxOffsetMap} {
        costVector = createVector(LogicalType::DOUBLE());
    }

    void beginWritingInternal(table_id_t tableID) override { costs->pinTable(tableID); }

    void write(FactorizedTable& fTable, table_id_t tableID, LimitCounter* counter) override {
        for (auto i = 0u; i < maxOffsetMap.at(tableID); ++i) {
            write(fTable, {i, tableID}, counter);
        }
    }

    void write(FactorizedTable& fTable, nodeID_t dstNodeID, LimitCounter* counter) override {
        if (!inOutputNodeMask(dstNodeID.offset)) { // Skip dst if it not is in scope.
            return;
        }
        if (dstNodeID == sourceNodeID_) { // Skip writing source node.
            return;
        }
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        auto cost = costs->getCost(dstNodeID.offset);
        if (cost == std::numeric_limits<double>::max()) { // Skip if dst is not visited.
            return;
        }
        costVector->setValue<double>(0, cost);
        fTable.append(vectors);
        if (counter != nullptr) {
            counter->increase(1);
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<WSPDestinationsOutputWriter>(context, outputNodeMask, sourceNodeID_,
            costs, maxOffsetMap);
    }

private:
    Costs* costs;
    std::unique_ptr<ValueVector> costVector;
    table_id_map_t<offset_t> maxOffsetMap;
};

class WeightedSPDestinationsAlgorithm : public RJAlgorithm {
public:
    std::string getFunctionName() const override { return WeightedSPDestinationsFunction::name; }

    // return srcNodeID, dstNodeID, weight
    expression_vector getResultColumns(const RJBindData& bindData) const override {
        expression_vector columns;
        columns.push_back(bindData.nodeInput->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData.nodeOutput->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData.weightOutputExpr);
        return columns;
    }

    std::unique_ptr<RJAlgorithm> copy() const override {
        return std::make_unique<WeightedSPDestinationsAlgorithm>(*this);
    }

private:
    std::unique_ptr<GDSComputeState> getComputeState(ExecutionContext* context,
        const RJBindData& bindData, RecursiveExtendSharedState* sharedState) override {
        auto clientContext = context->clientContext;
        auto graph = sharedState->graph.get();
        auto curDenseFrontier = DenseFrontier::getUninitializedFrontier(context, graph);
        auto nextDenseFrontier = DenseFrontier::getUninitializedFrontier(context, graph);
        auto frontierPair = std::make_unique<DenseSparseDynamicFrontierPair>(
            std::move(curDenseFrontier), std::move(nextDenseFrontier));
        auto costs =
            std::make_shared<Costs>(graph->getMaxOffsetMap(clientContext->getTransaction()),
                clientContext->getMemoryManager());
        auto auxiliaryState = std::make_unique<WSPDestinationsAuxiliaryState>(costs);
        std::unique_ptr<GDSComputeState> gdsState;
        visit(bindData.weightPropertyExpr->getDataType(), [&]<typename T>(T) {
            auto edgeCompute = std::make_unique<WSPDestinationsEdgeCompute<T>>(costs);
            gdsState = std::make_unique<GDSComputeState>(std::move(frontierPair),
                std::move(edgeCompute), std::move(auxiliaryState));
        });
        return gdsState;
    }

    std::unique_ptr<RJOutputWriter> getOutputWriter(ExecutionContext* context, const RJBindData&,
        GDSComputeState& computeState, nodeID_t sourceNodeID,
        RecursiveExtendSharedState* sharedState) override {
        auto costs =
            computeState.auxiliaryState->ptrCast<WSPDestinationsAuxiliaryState>()->getCosts();
        auto clientContext = context->clientContext;
        return std::make_unique<WSPDestinationsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, costs,
            sharedState->graph->getMaxOffsetMap(clientContext->getTransaction()));
    }
};

std::unique_ptr<RJAlgorithm> WeightedSPDestinationsFunction::getAlgorithm() {
    return std::make_unique<WeightedSPDestinationsAlgorithm>();
}

} // namespace function
} // namespace kuzu
