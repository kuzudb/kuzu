#include <utility>

#include "common/types/types.h"
#include "function/gds/bfs_graph.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "processor/execution_context.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

/**
 * A dense storage structures for multiplicities for multiple node tables.
 */
class PathMultiplicities {
public:
    PathMultiplicities(const std::unordered_map<table_id_t, uint64_t>& numNodesMap,
        storage::MemoryManager* mm) {
        for (auto& [tableID, numNodes] : numNodesMap) {
            multiplicityArray.allocate(tableID, numNodes, mm);
            // Question to Trevor: Do I need to use atomics? If so, why?
            auto data = multiplicityArray.getData(tableID);
            for (uint64_t i = 0; i < numNodes; ++i) {
                data[i].store(0);
            }
        }
    }

    void incrementTargetMultiplicity(offset_t offset, uint64_t multiplicity) {
        curTargetMultiplicities[offset].fetch_add(multiplicity);
    }

    uint64_t getBoundMultiplicity(offset_t nodeOffset) {
        return curBoundMultiplicities[nodeOffset].load(std::memory_order_relaxed);
    }

    uint64_t getTargetMultiplicity(offset_t nodeOffset) {
        return curTargetMultiplicities[nodeOffset].load(std::memory_order_relaxed);
    }

    void pinBoundTable(table_id_t tableID) {
        curBoundMultiplicities = multiplicityArray.getData(tableID);
    }

    void pinTargetTable(table_id_t tableID) {
        curTargetMultiplicities = multiplicityArray.getData(tableID);
    }

private:
    ObjectArraysMap<std::atomic<uint64_t>> multiplicityArray;
    // curTargetMultiplicities is the multiplicities of the current table that will be updated in a
    // particular rel table extension.
    std::atomic<uint64_t>* curTargetMultiplicities = nullptr;
    // curBoundMultiplicities is the multiplicities of the table "from which" an extension is
    // being made.
    std::atomic<uint64_t>* curBoundMultiplicities = nullptr;
};

struct AllSPDestinationOutputs : public RJOutputs {
    std::shared_ptr<PathLengths> pathLengths;
    std::shared_ptr<PathMultiplicities> multiplicities;

public:
    AllSPDestinationOutputs(nodeID_t sourceNodeID, std::shared_ptr<PathLengths> pathLengths,
        std::shared_ptr<PathMultiplicities> multiplicities)
        : RJOutputs{sourceNodeID}, pathLengths{std::move(pathLengths)},
          multiplicities{std::move(multiplicities)} {}

    void initRJFromSource(nodeID_t source) override {
        multiplicities->pinTargetTable(source.tableID);
        multiplicities->incrementTargetMultiplicity(source.offset, 1);
    }

    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) override {
        // Note: We do not fix the node table for pathLengths, which is inherited from AllSPOutputs.
        // See the comment in single_shortest_path.cpp for details.
        multiplicities->pinBoundTable(curFrontierTableID);
        multiplicities->pinTargetTable(nextFrontierTableID);
    };

    void beginWritingOutputsForDstNodesInTable(table_id_t tableID) override {
        pathLengths->pinCurFrontierTableID(tableID);
        multiplicities->pinTargetTable(tableID);
    }
};

class AllSPDestinationsOutputWriter : public RJOutputWriter {
public:
    AllSPDestinationsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
        NodeOffsetMaskMap* outputNodeMask)
        : RJOutputWriter{context, rjOutputs, outputNodeMask} {
        lengthVector = createVector(LogicalType::UINT16(), context->getMemoryManager());
    }

    void write(FactorizedTable& fTable, nodeID_t dstNodeID, GDSOutputCounter* counter) override {
        auto outputs = rjOutputs->ptrCast<AllSPDestinationOutputs>();
        auto length = outputs->pathLengths->getMaskValueFromCurFrontier(dstNodeID.offset);
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        lengthVector->setValue<uint16_t>(0, length);
        auto multiplicity = outputs->multiplicities->getTargetMultiplicity(dstNodeID.offset);
        for (auto i = 0u; i < multiplicity; ++i) {
            fTable.append(vectors);
        }
        if (counter != nullptr) {
            counter->increase(multiplicity);
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<AllSPDestinationsOutputWriter>(context, rjOutputs, outputNodeMask);
    }

private:
    bool skipInternal(nodeID_t dstNodeID) const override {
        auto outputs = rjOutputs->ptrCast<AllSPDestinationOutputs>();
        return dstNodeID == outputs->sourceNodeID ||
               outputs->pathLengths->getMaskValueFromCurFrontier(dstNodeID.offset) ==
                   PathLengths::UNVISITED;
    }

private:
    std::unique_ptr<ValueVector> lengthVector;
};

class AllSPDestinationsEdgeCompute : public SPEdgeCompute {
public:
    AllSPDestinationsEdgeCompute(SinglePathLengthsFrontierPair* frontierPair,
        std::shared_ptr<PathMultiplicities> multiplicities)
        : SPEdgeCompute{frontierPair}, multiplicities{std::move(multiplicities)} {};

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, NbrScanState::Chunk& resultChunk,
        bool) override {
        std::vector<nodeID_t> activeNodes;
        resultChunk.forEach([&](auto nbrNodeID, auto /*edgeID*/) {
            auto nbrVal =
                frontierPair->getPathLengths()->getMaskValueFromNextFrontier(nbrNodeID.offset);
            // We should update the nbrID's multiplicity in 2 cases: 1) if nbrID is being visited
            // for the first time, i.e., when its value in the pathLengths frontier is
            // PathLengths::UNVISITED. Or 2) if nbrID has already been visited but in this
            // iteration, so it's value is curIter + 1.
            auto shouldUpdate =
                nbrVal == PathLengths::UNVISITED || nbrVal == frontierPair->getCurrentIter();
            if (shouldUpdate) {
                // Note: This is safe because curNodeID is in the current frontier, so its
                // shortest paths multiplicity is guaranteed to not change in the current iteration.
                multiplicities->incrementTargetMultiplicity(nbrNodeID.offset,
                    multiplicities->getBoundMultiplicity(boundNodeID.offset));
            }
            if (nbrVal == PathLengths::UNVISITED) {
                activeNodes.push_back(nbrNodeID);
            }
        });
        return activeNodes;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<AllSPDestinationsEdgeCompute>(frontierPair, multiplicities);
    }

private:
    std::shared_ptr<PathMultiplicities> multiplicities;
};

class AllSPPathsEdgeCompute : public SPEdgeCompute {
public:
    AllSPPathsEdgeCompute(SinglePathLengthsFrontierPair* frontiersPair, BFSGraph* bfsGraph)
        : SPEdgeCompute{frontiersPair}, bfsGraph{bfsGraph} {
        parentListBlock = bfsGraph->addNewBlock();
    }

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, NbrScanState::Chunk& resultChunk,
        bool fwdEdge) override {
        std::vector<nodeID_t> activeNodes;
        resultChunk.forEach([&](auto nbrNodeID, auto edgeID) {
            auto nbrLen =
                frontierPair->getPathLengths()->getMaskValueFromNextFrontier(nbrNodeID.offset);
            // We should update in 2 cases: 1) if nbrID is being visited
            // for the first time, i.e., when its value in the pathLengths frontier is
            // PathLengths::UNVISITED. Or 2) if nbrID has already been visited but in this
            // iteration, so it's value is curIter + 1.
            auto shouldUpdate =
                nbrLen == PathLengths::UNVISITED || nbrLen == frontierPair->getCurrentIter();
            if (shouldUpdate) {
                if (!parentListBlock->hasSpace()) {
                    parentListBlock = bfsGraph->addNewBlock();
                }
                bfsGraph->addParent(frontierPair->getCurrentIter(), parentListBlock,
                    nbrNodeID /* child */, boundNodeID /* parent */, edgeID, fwdEdge);
            }
            if (nbrLen == PathLengths::UNVISITED) {
                activeNodes.push_back(nbrNodeID);
            }
        });
        return activeNodes;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<AllSPPathsEdgeCompute>(frontierPair, bfsGraph);
    }

private:
    BFSGraph* bfsGraph;
    ObjectBlock<ParentList>* parentListBlock = nullptr;
};

/**
 * Algorithm for parallel all shortest paths computation, so all shortest paths from a source to
 * is returned for each destination. If paths are not returned, multiplicities indicate the
 * number of paths to each destination.
 */

class AllSPDestinationsAlgorithm final : public SPAlgorithm {
public:
    AllSPDestinationsAlgorithm() = default;
    AllSPDestinationsAlgorithm(const AllSPDestinationsAlgorithm& other) : SPAlgorithm{other} {}

    expression_vector getResultColumns(Binder*) const override {
        auto columns = getBaseResultColumns();
        columns.push_back(bindData->ptrCast<RJBindData>()->lengthExpr);
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<AllSPDestinationsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto frontier = getPathLengthsFrontier(context, PathLengths::UNVISITED);
        auto numNodesMap = sharedState->graph->getNumNodesMap(clientContext->getTransaction());
        auto mm = clientContext->getMemoryManager();
        auto multiplicities = std::make_shared<PathMultiplicities>(numNodesMap, mm);
        auto output =
            std::make_unique<AllSPDestinationOutputs>(sourceNodeID, frontier, multiplicities);
        auto outputWriter = std::make_unique<AllSPDestinationsOutputWriter>(clientContext,
            output.get(), sharedState->getOutputNodeMaskMap());
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(frontier);
        auto edgeCompute =
            std::make_unique<AllSPDestinationsEdgeCompute>(frontierPair.get(), multiplicities);
        return RJCompState(std::move(frontierPair), std::move(edgeCompute),
            sharedState->getOutputNodeMaskMap(), std::move(output), std::move(outputWriter));
    }
};

class AllSPPathsAlgorithm final : public SPAlgorithm {
public:
    AllSPPathsAlgorithm() = default;
    AllSPPathsAlgorithm(const AllSPPathsAlgorithm& other) : SPAlgorithm{other} {}

    expression_vector getResultColumns(Binder*) const override {
        auto columns = getBaseResultColumns();
        auto rjBindData = bindData->ptrCast<RJBindData>();
        columns.push_back(rjBindData->lengthExpr);
        columns.push_back(rjBindData->pathNodeIDsExpr);
        columns.push_back(rjBindData->pathEdgeIDsExpr);
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<AllSPPathsAlgorithm>(*this);
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
            std::make_unique<AllSPPathsEdgeCompute>(frontierPair.get(), output->bfsGraph.get());
        return RJCompState(std::move(frontierPair), std::move(edgeCompute),
            sharedState->getOutputNodeMaskMap(), std::move(output), std::move(outputWriter));
    }
};

function_set AllSPDestinationsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(getFunction()));
    return result;
}

GDSFunction AllSPDestinationsFunction::getFunction() {
    auto algo = std::make_unique<AllSPDestinationsAlgorithm>();
    auto params = algo->getParameterTypeIDs();
    return GDSFunction(name, std::move(params), std::move(algo));
}

function_set AllSPPathsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(getFunction()));
    return result;
}

GDSFunction AllSPPathsFunction::getFunction() {
    auto algo = std::make_unique<AllSPPathsAlgorithm>();
    auto params = algo->getParameterTypeIDs();
    return GDSFunction(name, std::move(params), std::move(algo));
}

} // namespace function
} // namespace kuzu
