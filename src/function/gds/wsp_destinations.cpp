#include "binder/expression/node_expression.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "processor/execution_context.h"
#include "wsp_utils.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::storage;

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
    ObjectArraysMap<std::atomic<double>> costsMap;
};

template<typename T>
class WSPDestinationsEdgeCompute : public EdgeCompute {
public:
    explicit WSPDestinationsEdgeCompute(std::shared_ptr<Costs> costs) : costs{std::move(costs)} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        std::vector<nodeID_t> result;
        chunk.forEach<T>([&](auto nbrNodeID, auto /* edgeID */, auto weight) {
            if (costs->update(boundNodeID.offset, nbrNodeID.offset, (double)weight)) {
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

struct WSPDestinationsAuxiliaryState : public GDSAuxiliaryState {
public:
    explicit WSPDestinationsAuxiliaryState(std::shared_ptr<Costs> costs)
        : costs{std::move(costs)} {}

    void initSource(nodeID_t sourceNodeID) override { costs->setCost(sourceNodeID.offset, 0); }

    void beginFrontierCompute(table_id_t, table_id_t toTableID) override {
        costs->pinTable(toTableID);
    }

private:
    std::shared_ptr<Costs> costs;
};

class WSPDestinationsOutputWriter : public RJOutputWriter {
public:
    WSPDestinationsOutputWriter(main::ClientContext* context,
        common::NodeOffsetMaskMap* outputNodeMask, common::nodeID_t sourceNodeID,
        std::shared_ptr<Costs> costs)
        : RJOutputWriter{context, outputNodeMask, sourceNodeID}, costs{std::move(costs)} {
        costVector = createVector(LogicalType::DOUBLE());
    }

    void write(processor::FactorizedTable& fTable, nodeID_t dstNodeID,
        LimitCounter* counter) override {
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        auto cost = costs->getCost(dstNodeID.offset);
        costVector->setValue<double>(0, cost);
        fTable.append(vectors);
        if (counter != nullptr) {
            counter->increase(1);
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<WSPDestinationsOutputWriter>(context, outputNodeMask, sourceNodeID,
            costs);
    }

private:
    void beginWritingOutputsInternal(table_id_t tableID) override { costs->pinTable(tableID); }
    bool skipInternal(nodeID_t dstNodeID) const override {
        return dstNodeID == sourceNodeID ||
               costs->getCost(dstNodeID.offset) == std::numeric_limits<double>::max();
    }

private:
    std::shared_ptr<Costs> costs;
    std::unique_ptr<ValueVector> costVector;
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
    RJCompState getRJCompState(processor::ExecutionContext* context, nodeID_t sourceNodeID,
        const RJBindData& bindData, processor::RecursiveExtendSharedState* sharedState) override {
        auto clientContext = context->clientContext;
        auto graph = sharedState->graph.get();
        auto curFrontier = PathLengths::getUnvisitedFrontier(context, sharedState->graph.get());
        auto nextFrontier = PathLengths::getUnvisitedFrontier(context, sharedState->graph.get());
        auto frontierPair =
            std::make_unique<DoublePathLengthsFrontierPair>(curFrontier, nextFrontier);
        auto costs =
            std::make_shared<Costs>(graph->getMaxOffsetMap(clientContext->getTransaction()),
                clientContext->getMemoryManager());
        auto auxiliaryState = std::make_unique<WSPDestinationsAuxiliaryState>(costs);
        auto outputWriter = std::make_unique<WSPDestinationsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, costs);
        std::unique_ptr<GDSComputeState> gdsState;
        visit(bindData.weightPropertyExpr->getDataType(), [&]<typename T>(T) {
            auto edgeCompute = std::make_unique<WSPDestinationsEdgeCompute<T>>(costs);
            gdsState =
                std::make_unique<GDSComputeState>(std::move(frontierPair), std::move(edgeCompute),
                    std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        });
        return RJCompState(std::move(gdsState), std::move(outputWriter));
    }
};

std::unique_ptr<RJAlgorithm> WeightedSPDestinationsFunction::getAlgorithm() {
    return std::make_unique<WeightedSPDestinationsAlgorithm>();
}

} // namespace function
} // namespace kuzu
