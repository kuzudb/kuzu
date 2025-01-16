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

template<typename T>
class Weights {
public:
    Weights(const table_id_map_t<offset_t>& numNodesMap, MemoryManager* mm) {
        init(numNodesMap, mm);
    }

    void pinTable(table_id_t tableID) { weights = weightsMap.getData(tableID); }

    T getWeight(offset_t offset) { return weights[offset].load(std::memory_order_relaxed); }

    void setWeight(offset_t offset, T val) {
        weights[offset].store(val, std::memory_order_relaxed);
    }

    bool update(offset_t boundOffset, offset_t nbrOffset, T val) {
        auto newWeight = getWeight(boundOffset) + val;
        auto tmp = getWeight(nbrOffset);
        while (newWeight < tmp) {
            if (weights[nbrOffset].compare_exchange_strong(tmp, newWeight)) {
                return true;
            }
        }
        return false;
    }

private:
    void init(const table_id_map_t<offset_t>& numNodesMap, MemoryManager* mm) {
        for (const auto& [tableID, numNodes] : numNodesMap) {
            weightsMap.allocate(tableID, numNodes, mm);
            pinTable(tableID);
            for (auto i = 0u; i < numNodes; ++i) {
                weights[i].store(std::numeric_limits<T>::max(), std::memory_order_relaxed);
            }
        }
    }

private:
    std::atomic<T>* weights;
    ObjectArraysMap<std::atomic<T>> weightsMap;
};

template<typename T>
class DestinationsEdgeCompute : public EdgeCompute {
public:
    explicit DestinationsEdgeCompute(std::shared_ptr<Weights<T>> weights)
        : weights{std::move(weights)} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        std::vector<nodeID_t> result;
        chunk.forEach<T>([&](auto nbrNodeID, auto /* edgeID */, auto weight) {
            if (weights->update(boundNodeID.offset, nbrNodeID.offset, weight)) {
                result.push_back(nbrNodeID);
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<DestinationsEdgeCompute<T>>(weights);
    }

private:
    std::shared_ptr<Weights<T>> weights;
};

template<typename T>
struct DestinationOutputs : public RJOutputs {
    std::shared_ptr<Weights<T>> weights;

    DestinationOutputs(nodeID_t sourceNodeID, std::shared_ptr<Weights<T>> weights)
        : RJOutputs{sourceNodeID}, weights{std::move(weights)} {}

    void initRJFromSource(common::nodeID_t a) override { weights->setWeight(a.offset, 0); }

    void beginFrontierComputeBetweenTables(table_id_t, table_id_t nextFrontierTableID) override {
        weights->pinTable(nextFrontierTableID);
    }

    void beginWritingOutputsForDstNodesInTable(table_id_t tableID) override {
        weights->pinTable(tableID);
    }
};

template<typename T>
class DestinationsOutputWriter : public RJOutputWriter {
public:
    DestinationsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
        processor::NodeOffsetMaskMap* outputNodeMask, std::shared_ptr<Weights<T>> weights,
        const LogicalType& weightType)
        : RJOutputWriter{context, rjOutputs, outputNodeMask}, weights{std::move(weights)} {
        weightVector = createVector(weightType, context->getMemoryManager());
    }

    void write(processor::FactorizedTable& fTable, nodeID_t dstNodeID,
        processor::GDSOutputCounter* counter) override {
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        auto weight = weights->getWeight(dstNodeID.offset);
        weightVector->setValue<T>(0, weight);
        fTable.append(vectors);
        if (counter != nullptr) {
            counter->increase(1);
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<DestinationsOutputWriter<T>>(context, rjOutputs, outputNodeMask,
            weights, weightVector->dataType);
    }

private:
    bool skipInternal(common::nodeID_t dstNodeID) const override {
        auto weight = weights->getWeight(dstNodeID.offset);
        return dstNodeID == rjOutputs->sourceNodeID || weight == std::numeric_limits<T>::max();
    }

private:
    std::shared_ptr<Weights<T>> weights;
    std::unique_ptr<ValueVector> weightVector;
};

class WeightedSPDestinationsAlgorithm : public SPAlgorithm {
public:
    WeightedSPDestinationsAlgorithm() = default;
    WeightedSPDestinationsAlgorithm(const WeightedSPDestinationsAlgorithm& other)
        : SPAlgorithm{other} {}

    binder::expression_vector getResultColumns(binder::Binder*) const override {
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
        case LogicalTypeID::DECIMAL:
            switch (dataType.getPhysicalType()) {
            case PhysicalTypeID::INT16:
                return func(int16_t());
            case PhysicalTypeID::INT32:
                return func(int32_t());
            case PhysicalTypeID::INT64:
                return func(int64_t());
            default:
                break;
            }
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
        std::unique_ptr<EdgeCompute> edgeCompute;
        std::unique_ptr<RJOutputs> outputs;
        std::unique_ptr<RJOutputWriter> outputWriter;
        auto& dataType = rjBindData->weightOutputExpr->getDataType();
        visit(dataType, [&]<typename T>(T) {
            auto weight = std::make_shared<Weights<T>>(numNodes, clientContext->getMemoryManager());
            edgeCompute = std::make_unique<DestinationsEdgeCompute<T>>(weight);
            outputs = std::make_unique<DestinationOutputs<T>>(sourceNodeID, weight);
            outputWriter = std::make_unique<DestinationsOutputWriter<T>>(clientContext,
                outputs.get(), sharedState->getOutputNodeMaskMap(), weight, dataType);
        });
        return RJCompState(std::move(frontierPair), std::move(edgeCompute),
            sharedState->getOutputNodeMaskMap(), std::move(outputs), std::move(outputWriter));
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
