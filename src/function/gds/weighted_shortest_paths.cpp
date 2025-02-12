#include "binder/binder.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "function/gds_function.h"
#include "main/client_context.h"
#include "processor/execution_context.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

class Costs {
public:
    Costs(const table_id_map_t<offset_t>& numNodesMap, MemoryManager* mm) {
        init(numNodesMap, mm);
    }

    void pinTable(table_id_t tableID) { costs = costsMap.getData(tableID); }

    double getWeight(offset_t offset) { return costs[offset].load(std::memory_order_relaxed); }

    void setWeight(offset_t offset, double val) {
        costs[offset].store(val, std::memory_order_relaxed);
    }

    bool update(offset_t boundOffset, offset_t nbrOffset, double val) {
        auto newWeight = getWeight(boundOffset) + val;
        auto tmp = getWeight(nbrOffset);
        while (newWeight < tmp) {
            if (costs[nbrOffset].compare_exchange_strong(tmp, newWeight)) {
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
class DestinationsEdgeCompute : public EdgeCompute {
public:
    explicit DestinationsEdgeCompute(std::shared_ptr<Costs> costs)
        : costs{std::move(costs)} {}

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
        return std::make_unique<DestinationsEdgeCompute<T>>(costs);
    }

private:
    std::shared_ptr<Costs> costs;
};

struct WeightedSPDestinationsAuxiliaryState : public GDSAuxiliaryState {
public:
    explicit WeightedSPDestinationsAuxiliaryState(std::shared_ptr<Costs> costs)
        : costs{std::move(costs)} {}

    void initSource(nodeID_t sourceNodeID) override { costs->setWeight(sourceNodeID.offset, 0); }

    void beginFrontierCompute(table_id_t, table_id_t toTableID) override {
        costs->pinTable(toTableID);
    }

private:
    std::shared_ptr<Costs> costs;
};

class DestinationsOutputWriter : public RJOutputWriter {
public:
    DestinationsOutputWriter(main::ClientContext* context,
        processor::NodeOffsetMaskMap* outputNodeMask, common::nodeID_t sourceNodeID,
        std::shared_ptr<Costs> costs)
        : RJOutputWriter{context, outputNodeMask, sourceNodeID}, costs{std::move(costs)} {
        costVector = createVector(LogicalType::DOUBLE(), context->getMemoryManager());
    }

    void write(processor::FactorizedTable& fTable, nodeID_t dstNodeID,
        processor::GDSOutputCounter* counter) override {
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        auto weight = costs->getWeight(dstNodeID.offset);
        costVector->setValue<double>(0, weight);
        fTable.append(vectors);
        if (counter != nullptr) {
            counter->increase(1);
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<DestinationsOutputWriter>(context, outputNodeMask, sourceNodeID,
            costs);
    }

private:
    void beginWritingOutputsInternal(table_id_t tableID) override { costs->pinTable(tableID); }
    bool skipInternal(nodeID_t dstNodeID) const override {
        auto weight = costs->getWeight(dstNodeID.offset);
        return dstNodeID == sourceNodeID || weight == std::numeric_limits<double>::max();
    }

private:
    std::shared_ptr<Costs> costs;
    std::unique_ptr<ValueVector> costVector;
};

class WeightedSPDestinationsAlgorithm : public SPAlgorithm {
public:
    WeightedSPDestinationsAlgorithm() = default;
    WeightedSPDestinationsAlgorithm(const WeightedSPDestinationsAlgorithm& other)
        : SPAlgorithm{other} {}

    binder::expression_vector getResultColumns(
        const function::GDSBindInput& /*bindInput*/) const override {
        auto columns = getBaseResultColumns();
        columns.push_back(bindData->ptrCast<RJBindData>()->weightOutputExpr);
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<WeightedSPDestinationsAlgorithm>(*this);
    }

private:
    template<typename... Fs>
    static auto visit(const LogicalType& dataType, Fs... funcs) {
        auto func = overload(funcs...);
        switch (dataType.getLogicalTypeID()) {
        /* NOLINTBEGIN(bugprone-branch-clone)*/
        case LogicalTypeID::INT8:
            return func(int8_t());
        case LogicalTypeID::UINT8:
            return func(uint8_t());
        case LogicalTypeID::INT16:
            return func(int16_t());
        case LogicalTypeID::UINT16:
            return func(uint16_t());
        case LogicalTypeID::INT32:
            return func(int32_t());
        case LogicalTypeID::UINT32:
            return func(uint32_t());
        case LogicalTypeID::INT64:
            return func(int64_t());
        case LogicalTypeID::UINT64:
            return func(uint64_t());
        case LogicalTypeID::DOUBLE:
            return func(double());
        case LogicalTypeID::FLOAT:
            return func(float());
        /* NOLINTEND(bugprone-branch-clone)*/
        default:
            break;
        }
        throw RuntimeException(stringFormat(
            "{} weight type is not supported for weighted shortest path.", dataType.toString()));
    }

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
        auto auxiliaryState = std::make_unique<WeightedSPDestinationsAuxiliaryState>(costs);
        auto outputWriter = std::make_unique<DestinationsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, costs);
        std::unique_ptr<GDSComputeState> gdsState;
        visit(rjBindData->weightPropertyExpr->getDataType(), [&]<typename T>(T) {
            auto edgeCompute = std::make_unique<DestinationsEdgeCompute<T>>(costs);
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
