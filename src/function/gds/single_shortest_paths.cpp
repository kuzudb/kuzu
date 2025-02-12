#include "common/types/types.h"
#include "function/gds/auxiliary_state/path_auxiliary_state.h"
#include "function/gds/bfs_graph.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "processor/execution_context.h"
#include "binder/expression/node_expression.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

class SingleSPDestinationsOutputWriter : public RJOutputWriter {
public:
    SingleSPDestinationsOutputWriter(main::ClientContext* context,
        NodeOffsetMaskMap* outputNodeMask, nodeID_t sourceNodeID,
        std::shared_ptr<PathLengths> pathLengths)
        : RJOutputWriter{context, outputNodeMask, sourceNodeID},
          pathLengths{std::move(pathLengths)} {
        lengthVector = createVector(LogicalType::UINT16(), context->getMemoryManager());
    }

    void beginWritingOutputsInternal(common::table_id_t tableID) override {
        pathLengths->pinCurFrontierTableID(tableID);
    }

    void write(FactorizedTable& fTable, nodeID_t dstNodeID, GDSOutputCounter* counter) override {
        auto length = pathLengths->getMaskValueFromCurFrontier(dstNodeID.offset);
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        lengthVector->setValue<uint16_t>(0, length);
        fTable.append(vectors);
        if (counter != nullptr) {
            counter->increase(1);
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<SingleSPDestinationsOutputWriter>(context, outputNodeMask,
            sourceNodeID, pathLengths);
    }

private:
    bool skipInternal(nodeID_t dstNodeID) const override {
        return dstNodeID == sourceNodeID ||
               pathLengths->getMaskValueFromCurFrontier(dstNodeID.offset) == PathLengths::UNVISITED;
    }

private:
    std::unique_ptr<ValueVector> lengthVector;
    std::shared_ptr<PathLengths> pathLengths;
};

class SingleSPDestinationsEdgeCompute : public SPEdgeCompute {
public:
    explicit SingleSPDestinationsEdgeCompute(SinglePathLengthsFrontierPair* frontierPair)
        : SPEdgeCompute{frontierPair} {};

    std::vector<nodeID_t> edgeCompute(nodeID_t, NbrScanState::Chunk& resultChunk, bool) override {
        std::vector<nodeID_t> activeNodes;
        resultChunk.forEach([&](auto nbrNode, auto) {
            if (frontierPair->getPathLengths()->getMaskValueFromNextFrontier(nbrNode.offset) ==
                PathLengths::UNVISITED) {
                activeNodes.push_back(nbrNode);
            }
        });
        return activeNodes;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<SingleSPDestinationsEdgeCompute>(frontierPair);
    }
};

class SingleSPPathsEdgeCompute : public SPEdgeCompute {
public:
    SingleSPPathsEdgeCompute(SinglePathLengthsFrontierPair* frontierPair, BFSGraph* bfsGraph)
        : SPEdgeCompute{frontierPair}, bfsGraph{bfsGraph} {
        block = bfsGraph->addNewBlock();
    }

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, NbrScanState::Chunk& resultChunk,
        bool isFwd) override {
        std::vector<nodeID_t> activeNodes;
        resultChunk.forEach([&](auto nbrNodeID, auto edgeID) {
            if (frontierPair->getPathLengths()->getMaskValueFromNextFrontier(nbrNodeID.offset) ==
                PathLengths::UNVISITED) {
                if (!block->hasSpace()) {
                    block = bfsGraph->addNewBlock();
                }
                bfsGraph->addSingleParent(frontierPair->getCurrentIter(), boundNodeID, edgeID,
                    nbrNodeID, isFwd, block);
                activeNodes.push_back(nbrNodeID);
            }
        });
        return activeNodes;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<SingleSPPathsEdgeCompute>(frontierPair, bfsGraph);
    }

private:
    BFSGraph* bfsGraph;
    ObjectBlock<ParentList>* block = nullptr;
};

/**
 * Algorithm for parallel single shortest path computation, i.e., assumes Distinct semantics, so
 * one arbitrary shortest path is returned for each destination. If paths are not returned,
 * multiplicities of each destination is ignored (e.g., if there are 3 paths to a destination d,
 * d is returned only once).
 */
class SingleSPDestinationsAlgorithm : public RJAlgorithm {
public:
    SingleSPDestinationsAlgorithm() = default;
    SingleSPDestinationsAlgorithm(const SingleSPDestinationsAlgorithm& other)
        : RJAlgorithm{other} {}

    expression_vector getResultColumns(const function::GDSBindInput& /*bindInput*/) const override {
        expression_vector columns;
        columns.push_back(bindData->getNodeInput()->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData->getNodeOutput()->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData->ptrCast<RJBindData>()->lengthExpr);
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<SingleSPDestinationsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto frontier = getPathLengthsFrontier(context, PathLengths::UNVISITED);
        auto outputWriter = std::make_unique<SingleSPDestinationsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, frontier);
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(frontier);
        auto edgeCompute = std::make_unique<SingleSPDestinationsEdgeCompute>(frontierPair.get());
        auto auxiliaryState = std::make_unique<EmptyGDSAuxiliaryState>();
        auto gdsState = std::make_unique<GDSComputeState>(std::move(frontierPair),
            std::move(edgeCompute), std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        return RJCompState(std::move(gdsState), std::move(outputWriter));
    }
};

class SingleSPPathsAlgorithm : public RJAlgorithm {
public:
    SingleSPPathsAlgorithm() = default;
    SingleSPPathsAlgorithm(const SingleSPPathsAlgorithm& other) : RJAlgorithm{other} {}

    expression_vector getResultColumns(const function::GDSBindInput& /*bindInput*/) const override {
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
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<SingleSPPathsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto frontier = getPathLengthsFrontier(context, PathLengths::UNVISITED);
        auto bfsGraph = getBFSGraph(context);
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto writerInfo = rjBindData->getPathWriterInfo();
        writerInfo.pathNodeMask = sharedState->getPathNodeMaskMap();
        auto outputWriter = std::make_unique<SPPathsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, writerInfo, *bfsGraph);
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(frontier);
        auto edgeCompute =
            std::make_unique<SingleSPPathsEdgeCompute>(frontierPair.get(), bfsGraph.get());
        auto auxiliaryState = std::make_unique<PathAuxiliaryState>(std::move(bfsGraph));
        auto gdsState = std::make_unique<GDSComputeState>(std::move(frontierPair),
            std::move(edgeCompute), std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        return RJCompState(std::move(gdsState), std::move(outputWriter));
    }
};

function_set SingleSPDestinationsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(getFunction()));
    return result;
}

GDSFunction SingleSPDestinationsFunction::getFunction() {
    auto algo = std::make_unique<SingleSPDestinationsAlgorithm>();
    auto params = algo->getParameterTypeIDs();
    return GDSFunction(name, std::move(params), std::move(algo));
}

function_set SingleSPPathsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(getFunction()));
    return result;
}

GDSFunction SingleSPPathsFunction::getFunction() {
    auto algo = std::make_unique<SingleSPPathsAlgorithm>();
    auto params = algo->getParameterTypeIDs();
    return GDSFunction(name, std::move(params), std::move(algo));
}

} // namespace function
} // namespace kuzu
