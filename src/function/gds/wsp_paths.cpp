#include "binder/binder.h"
#include "function/gds/auxiliary_state/path_auxiliary_state.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "function/gds_function.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "wsp_utils.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::processor;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

template<typename T>
class WSPPathsEdgeCompute : public EdgeCompute {
public:
    explicit WSPPathsEdgeCompute(BFSGraph& bfsGraph) : bfsGraph{bfsGraph} {
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
        return std::make_unique<WSPPathsEdgeCompute<T>>(bfsGraph);
    }

private:
    BFSGraph& bfsGraph;
    ObjectBlock<ParentList>* block = nullptr;
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

class WeightedSPPathsAlgorithm : public RJAlgorithm {
public:
    WeightedSPPathsAlgorithm() = default;
    WeightedSPPathsAlgorithm(const WeightedSPPathsAlgorithm& other) : RJAlgorithm{other} {}

    // return srcNodeID, dstNodeID, length, [direction], pathNodeIDs, pathEdgeIDs, weight
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
        auto graph = sharedState->graph.get();
        auto curFrontier = PathLengths::getUnvisitedFrontier(context, graph);
        auto nextFrontier = PathLengths::getUnvisitedFrontier(context, graph);
        auto frontierPair =
            std::make_unique<DoublePathLengthsFrontierPair>(curFrontier, nextFrontier);
        auto bfsGraph = getBFSGraph(context);
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto writerInfo = rjBindData->getPathWriterInfo();
        auto outputWriter = std::make_unique<WSPPathsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, writerInfo, *bfsGraph);
        std::unique_ptr<GDSComputeState> gdsState;
        visit(rjBindData->weightPropertyExpr->getDataType(), [&]<typename T>(T) {
            auto edgeCompute = std::make_unique<WSPPathsEdgeCompute<T>>(*bfsGraph);
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

} // namespace function
} // namespace kuzu
