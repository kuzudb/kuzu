#include "common/types/types.h"
#include "function/gds/bfs_graph.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "processor/execution_context.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

struct SingleSPDestinationOutputs : public RJOutputs {
    std::shared_ptr<PathLengths> pathLengths;

    SingleSPDestinationOutputs(nodeID_t sourceNodeID, std::shared_ptr<PathLengths> pathLengths)
        : RJOutputs{sourceNodeID}, pathLengths{std::move(pathLengths)} {}

    // Note: We do not fix the node table for pathLengths, because PathLengths is a
    // FrontierPair implementation and RJCompState will call beginFrontierComputeBetweenTables
    // on FrontierPair (and RJOutputs).
    void beginFrontierComputeBetweenTables(table_id_t, table_id_t) override{};

    void beginWritingOutputsForDstNodesInTable(table_id_t tableID) override {
        pathLengths->pinCurFrontierTableID(tableID);
    }
};

class SingleSPDestinationsOutputWriter : public RJOutputWriter {
public:
    SingleSPDestinationsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
        NodeOffsetMaskMap* outputNodeMask)
        : RJOutputWriter{context, rjOutputs, outputNodeMask} {
        lengthVector = createVector(LogicalType::UINT16(), context->getMemoryManager());
    }

    void write(FactorizedTable& fTable, nodeID_t dstNodeID, GDSOutputCounter* counter) override {
        auto outputs = rjOutputs->ptrCast<SingleSPDestinationOutputs>();
        auto length = outputs->pathLengths->getMaskValueFromCurFrontier(dstNodeID.offset);
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        lengthVector->setValue<uint16_t>(0, length);
        fTable.append(vectors);
        if (counter != nullptr) {
            counter->increase(1);
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<SingleSPDestinationsOutputWriter>(context, rjOutputs,
            outputNodeMask);
    }

private:
    bool skipInternal(nodeID_t dstNodeID) const override {
        auto outputs = rjOutputs->ptrCast<SingleSPDestinationOutputs>();
        return dstNodeID == outputs->sourceNodeID ||
               outputs->pathLengths->getMaskValueFromCurFrontier(dstNodeID.offset) ==
                   PathLengths::UNVISITED;
    }

private:
    std::unique_ptr<ValueVector> lengthVector;
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
        parentListBlock = bfsGraph->addNewBlock();
    }

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, NbrScanState::Chunk& resultChunk,
        bool isFwd) override {
        std::vector<nodeID_t> activeNodes;
        resultChunk.forEach([&](auto nbrNodeID, auto edgeID) {
            if (frontierPair->getPathLengths()->getMaskValueFromNextFrontier(nbrNodeID.offset) ==
                PathLengths::UNVISITED) {
                if (!parentListBlock->hasSpace()) {
                    parentListBlock = bfsGraph->addNewBlock();
                }
                bfsGraph->tryAddSingleParent(frontierPair->getCurrentIter(), parentListBlock,
                    nbrNodeID /* child */, boundNodeID /* parent */, edgeID, isFwd);
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
    ObjectBlock<ParentList>* parentListBlock = nullptr;
};

/**
 * Algorithm for parallel single shortest path computation, i.e., assumes Distinct semantics, so
 * one arbitrary shortest path is returned for each destination. If paths are not returned,
 * multiplicities of each destination is ignored (e.g., if there are 3 paths to a destination d,
 * d is returned only once).
 */
class SingleSPDestinationsAlgorithm : public SPAlgorithm {
public:
    SingleSPDestinationsAlgorithm() = default;
    SingleSPDestinationsAlgorithm(const SingleSPDestinationsAlgorithm& other)
        : SPAlgorithm{other} {}

    expression_vector getResultColumns(const function::GDSBindInput& /*bindInput*/) const override {
        auto columns = getBaseResultColumns();
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
        auto output = std::make_unique<SingleSPDestinationOutputs>(sourceNodeID, frontier);
        auto outputWriter = std::make_unique<SingleSPDestinationsOutputWriter>(clientContext,
            output.get(), sharedState->getOutputNodeMaskMap());
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(output->pathLengths);
        auto edgeCompute = std::make_unique<SingleSPDestinationsEdgeCompute>(frontierPair.get());
        return RJCompState(std::move(frontierPair), std::move(edgeCompute),
            sharedState->getOutputNodeMaskMap(), std::move(output), std::move(outputWriter));
    }
};

class SingleSPPathsAlgorithm : public SPAlgorithm {
public:
    SingleSPPathsAlgorithm() = default;
    SingleSPPathsAlgorithm(const SingleSPPathsAlgorithm& other) : SPAlgorithm{other} {}

    expression_vector getResultColumns(const function::GDSBindInput& /*bindInput*/) const override {
        auto columns = getBaseResultColumns();
        auto rjBindData = bindData->ptrCast<RJBindData>();
        columns.push_back(rjBindData->lengthExpr);
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
        auto output = std::make_unique<PathsOutputs>(sourceNodeID, std::move(bfsGraph));
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto writerInfo = rjBindData->getPathWriterInfo();
        writerInfo.pathNodeMask = sharedState->getPathNodeMaskMap();
        auto outputWriter = std::make_unique<SPPathsOutputWriter>(clientContext, output.get(),
            sharedState->getOutputNodeMaskMap(), writerInfo);
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(frontier);
        auto edgeCompute =
            std::make_unique<SingleSPPathsEdgeCompute>(frontierPair.get(), output->bfsGraph.get());
        return RJCompState(std::move(frontierPair), std::move(edgeCompute),
            sharedState->getOutputNodeMaskMap(), std::move(output), std::move(outputWriter));
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
