#include "binder/binder.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "function/gds_function.h"
#include "main/client_context.h"
#include "processor/execution_context.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::processor;
using namespace kuzu::binder;

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
class DestinationsEdgeCompute : public EdgeCompute {
public:
    explicit DestinationsEdgeCompute(std::shared_ptr<Costs> costs) : costs{std::move(costs)} {}

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

template<typename T>
class PathsEdgeCompute : public EdgeCompute {
public:
    explicit PathsEdgeCompute(BFSGraph& bfsGraph) : bfsGraph{bfsGraph} {
        block = bfsGraph.addNewBlock();
    }

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool fwdEdge) override {
        std::vector<nodeID_t> result;
        chunk.forEach<T>([&](auto nbrNodeID, auto edgeID, auto weight) {
            if (!block->hasSpace()) {
                block = bfsGraph.addNewBlock();
            }
            if (bfsGraph.tryAddSingleParentWithWeight(boundNodeID, edgeID, nbrNodeID, fwdEdge,
                    (double)weight, block)) {
                result.push_back(nbrNodeID);
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<PathsEdgeCompute<T>>(bfsGraph);
    }

private:
    BFSGraph& bfsGraph;
    ObjectBlock<ParentList>* block = nullptr;
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

struct WSPPathsAuxiliaryState : public GDSAuxiliaryState {
public:
    explicit WSPPathsAuxiliaryState(std::unique_ptr<BFSGraph> bfsGraph)
        : bfsGraph{std::move(bfsGraph)} {}

    void initSource(nodeID_t sourceNodeID) override {
        sourceParent.setCost(0);
        bfsGraph->setParentList(sourceNodeID, &sourceParent);
    }

    void beginFrontierCompute(table_id_t, table_id_t toTableID) override {
        bfsGraph->pinTableID(toTableID);
    }

private:
    std::unique_ptr<BFSGraph> bfsGraph;
    ParentList sourceParent;
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

class WSPPathsOutputWriter : public PathsOutputWriter {
public:
    WSPPathsOutputWriter(main::ClientContext* context, processor::NodeOffsetMaskMap* outputNodeMask,
        common::nodeID_t sourceNodeID, PathsOutputWriterInfo info, BFSGraph& bfsGraph)
        : PathsOutputWriter{context, outputNodeMask, sourceNodeID, info, bfsGraph} {
        costVector = createVector(LogicalType::DOUBLE(), context->getMemoryManager());
    }

    void write(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        processor::GDSOutputCounter* counter) override {
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        auto parent = bfsGraph.getParentListHead(dstNodeID.offset);
        costVector->setValue<double>(0, parent->getCost());
        std::vector<ParentList*> curPath;
        curPath.push_back(parent);
        while (parent->getCost() != 0) {
            parent = bfsGraph.getParentListHead(parent->getNodeID().offset);
            curPath.push_back(parent);
        }
        curPath.pop_back();
        writePath(curPath);
        fTable.append(vectors);
        updateCounterAndTerminate(counter);
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<WSPPathsOutputWriter>(context, outputNodeMask, sourceNodeID, info,
            bfsGraph);
    }

private:
    bool skipInternal(nodeID_t dstNodeID) const override {
        if (dstNodeID == sourceNodeID) {
            return true;
        }
        auto parent = bfsGraph.getParentListHead(dstNodeID.offset);
        return parent == nullptr || parent->getCost() == std::numeric_limits<double>::max();
    }

private:
    std::unique_ptr<ValueVector> costVector;
};

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
    // LCOV_EXCL_START
    throw RuntimeException(stringFormat(
        "{} weight type is not supported for weighted shortest path.", dataType.toString()));
    // LCOV_EXCL_STOP
}

class WeightedSPDestinationsAlgorithm : public RJAlgorithm {
public:
    WeightedSPDestinationsAlgorithm() = default;
    WeightedSPDestinationsAlgorithm(const WeightedSPDestinationsAlgorithm& other)
        : RJAlgorithm{other} {}

    binder::expression_vector getResultColumns(
        const function::GDSBindInput& /*bindInput*/) const override {
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
            auto edgeCompute = std::make_unique<DestinationsEdgeCompute<T>>(costs);
            gdsState =
                std::make_unique<GDSComputeState>(std::move(frontierPair), std::move(edgeCompute),
                    std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        });
        return RJCompState(std::move(gdsState), std::move(outputWriter));
    }
};

class WeightedSPPathsAlgorithm : public RJAlgorithm {
public:
    WeightedSPPathsAlgorithm() = default;
    WeightedSPPathsAlgorithm(const WeightedSPPathsAlgorithm& other) : RJAlgorithm{other} {}

    binder::expression_vector getResultColumns(
        const function::GDSBindInput& /*bindInput*/) const override {
        auto rjBindData = bindData->ptrCast<RJBindData>();
        expression_vector columns;
        columns.push_back(bindData->getNodeInput()->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData->getNodeOutput()->constCast<NodeExpression>().getInternalID());
        columns.push_back(rjBindData->lengthExpr);
        if (rjBindData->extendDirection == ExtendDirection::BOTH) {
            columns.push_back(rjBindData->directionExpr);
        }
        columns.push_back(rjBindData->pathNodeIDsExpr);
        columns.push_back(rjBindData->pathEdgeIDsExpr);
        columns.push_back(rjBindData->weightOutputExpr);
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<WeightedSPPathsAlgorithm>(*this);
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
        auto bfsGraph = getBFSGraph(context);
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto writerInfo = rjBindData->getPathWriterInfo();
        auto outputWriter = std::make_unique<WSPPathsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, writerInfo, *bfsGraph);
        std::unique_ptr<GDSComputeState> gdsState;
        visit(rjBindData->weightPropertyExpr->getDataType(), [&]<typename T>(T) {
            auto edgeCompute = std::make_unique<PathsEdgeCompute<T>>(*bfsGraph);
            auto auxiliaryState = std::make_unique<WSPPathsAuxiliaryState>(std::move(bfsGraph));
            gdsState =
                std::make_unique<GDSComputeState>(std::move(frontierPair), std::move(edgeCompute),
                    std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        });
        return RJCompState(std::move(gdsState), std::move(outputWriter));
    }
};

function_set WeightedSPPathsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(getFunction()));
    return result;
}

GDSFunction WeightedSPPathsFunction::getFunction() {
    auto algo = std::make_unique<WeightedSPPathsAlgorithm>();
    auto params = algo->getParameterTypeIDs();
    return GDSFunction(name, std::move(params), std::move(algo));
}

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
