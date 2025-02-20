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
    Costs(const table_id_map_t<offset_t>& numNodesMap, MemoryManager* mm) { init(numNodesMap, mm); }

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
    void init(const table_id_map_t<offset_t>& numNodesMap, MemoryManager* mm) {
        for (const auto& [tableID, numNodes] : numNodesMap) {
            costsMap.allocate(tableID, numNodes, mm);
            pinTable(tableID);
            for (auto i = 0u; i < numNodes; ++i) {
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
        processor::NodeOffsetMaskMap* outputNodeMask, common::nodeID_t sourceNodeID,
        std::shared_ptr<Costs> costs)
        : RJOutputWriter{context, outputNodeMask, sourceNodeID}, costs{std::move(costs)} {
        costVector = createVector(LogicalType::DOUBLE(), context->getMemoryManager());
    }

    void write(processor::FactorizedTable& fTable, nodeID_t dstNodeID,
        processor::GDSOutputCounter* counter) override {
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
    WeightedSPDestinationsAlgorithm() = default;
    WeightedSPDestinationsAlgorithm(const WeightedSPDestinationsAlgorithm& other)
        : RJAlgorithm{other} {}

    // return srcNodeID, dstNodeID, weight
    expression_vector getResultColumns(const GDSBindInput&) const override {
        expression_vector columns;
        columns.push_back(bindData->getNodeInput()->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData->getNodeOutput()->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData->ptrCast<RJBindData>()->weightOutputExpr);
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<WeightedSPDestinationsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(processor::ExecutionContext* context,
        nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto numNodes = sharedState->graph->getNumNodesMap(clientContext->getTransaction());
        auto curFrontier = getPathLengthsFrontier(context, PathLengths::UNVISITED);
        auto nextFrontier = getPathLengthsFrontier(context, PathLengths::UNVISITED);
        auto frontierPair =
            std::make_unique<DoublePathLengthsFrontierPair>(curFrontier, nextFrontier);
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto costs = std::make_shared<Costs>(numNodes, clientContext->getMemoryManager());
        auto auxiliaryState = std::make_unique<WSPDestinationsAuxiliaryState>(costs);
        auto outputWriter = std::make_unique<WSPDestinationsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, costs);
        std::unique_ptr<GDSComputeState> gdsState;
        visit(rjBindData->weightPropertyExpr->getDataType(), [&]<typename T>(T) {
            auto edgeCompute = std::make_unique<WSPDestinationsEdgeCompute<T>>(costs);
            gdsState =
                std::make_unique<GDSComputeState>(std::move(frontierPair), std::move(edgeCompute),
                    std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        });
        return RJCompState(std::move(gdsState), std::move(outputWriter));
    }
};

function_set WeightedSPDestinationsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(getFunction()));
    return result;
}

GDSFunction WeightedSPDestinationsFunction::getFunction() {
    auto algo = std::make_unique<WeightedSPDestinationsAlgorithm>();
    auto params = algo->getParameterTypeIDs();
    return GDSFunction(name, std::move(params), std::move(algo));
}

} // namespace function
} // namespace kuzu
