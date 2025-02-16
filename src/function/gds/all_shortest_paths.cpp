#include <utility>

#include "binder/expression/node_expression.h"
#include "common/types/types.h"
#include "function/gds/auxiliary_state/path_auxiliary_state.h"
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

class AllSPDestinationsAuxiliaryState : public GDSAuxiliaryState {
public:
    explicit AllSPDestinationsAuxiliaryState(std::shared_ptr<PathMultiplicities> multiplicities)
        : multiplicities{std::move(multiplicities)} {}

    void initSource(common::nodeID_t source) override {
        multiplicities->pinTargetTable(source.tableID);
        multiplicities->incrementTargetMultiplicity(source.offset, 1);
    }

    void beginFrontierCompute(common::table_id_t fromTableID,
        common::table_id_t toTableID) override {
        multiplicities->pinBoundTable(fromTableID);
        multiplicities->pinTargetTable(toTableID);
    }

private:
    std::shared_ptr<PathMultiplicities> multiplicities;
};

class AllSPDestinationsOutputWriter : public RJOutputWriter {
public:
    AllSPDestinationsOutputWriter(main::ClientContext* context, NodeOffsetMaskMap* outputNodeMask,
        nodeID_t sourceNodeID, std::shared_ptr<PathLengths> pathLengths,
        std::shared_ptr<PathMultiplicities> multiplicities)
        : RJOutputWriter{context, outputNodeMask, sourceNodeID},
          pathLengths{std::move(pathLengths)}, multiplicities{std::move(multiplicities)} {
        lengthVector = createVector(LogicalType::UINT16(), context->getMemoryManager());
    }

    void beginWritingOutputsInternal(common::table_id_t tableID) override {
        pathLengths->pinCurFrontierTableID(tableID);
        multiplicities->pinTargetTable(tableID);
    }

    void write(FactorizedTable& fTable, nodeID_t dstNodeID, GDSOutputCounter* counter) override {
        auto length = pathLengths->getMaskValueFromCurFrontier(dstNodeID.offset);
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        lengthVector->setValue<uint16_t>(0, length);
        auto multiplicity = multiplicities->getTargetMultiplicity(dstNodeID.offset);
        for (auto i = 0u; i < multiplicity; ++i) {
            fTable.append(vectors);
        }
        if (counter != nullptr) {
            counter->increase(multiplicity);
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<AllSPDestinationsOutputWriter>(context, outputNodeMask,
            sourceNodeID, pathLengths, multiplicities);
    }

private:
    bool skipInternal(nodeID_t dstNodeID) const override {
        return dstNodeID == sourceNodeID ||
               pathLengths->getMaskValueFromCurFrontier(dstNodeID.offset) == PathLengths::UNVISITED;
    }

private:
    std::unique_ptr<ValueVector> lengthVector;
    std::shared_ptr<PathLengths> pathLengths;
    std::shared_ptr<PathMultiplicities> multiplicities;
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
        block = bfsGraph->addNewBlock();
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
                if (!block->hasSpace()) {
                    block = bfsGraph->addNewBlock();
                }
                bfsGraph->addParent(frontierPair->getCurrentIter(), boundNodeID, edgeID, nbrNodeID,
                    fwdEdge, block);
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
    ObjectBlock<ParentList>* block = nullptr;
};

/**
 * Algorithm for parallel all shortest paths computation, so all shortest paths from a source to
 * is returned for each destination. If paths are not returned, multiplicities indicate the
 * number of paths to each destination.
 */
class AllSPDestinationsAlgorithm final : public RJAlgorithm {
public:
    AllSPDestinationsAlgorithm() = default;
    AllSPDestinationsAlgorithm(const AllSPDestinationsAlgorithm& other) : RJAlgorithm{other} {}

    expression_vector getResultColumns(const function::GDSBindInput& /*bindInput*/) const override {
        expression_vector columns;
        columns.push_back(bindData->getNodeInput()->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData->getNodeOutput()->constCast<NodeExpression>().getInternalID());
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
        auto outputWriter = std::make_unique<AllSPDestinationsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, frontier, multiplicities);
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(frontier);
        auto edgeCompute =
            std::make_unique<AllSPDestinationsEdgeCompute>(frontierPair.get(), multiplicities);
        auto auxiliaryState = std::make_unique<AllSPDestinationsAuxiliaryState>(multiplicities);
        auto gdsState = std::make_unique<GDSComputeState>(std::move(frontierPair),
            std::move(edgeCompute), std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        return RJCompState(std::move(gdsState), std::move(outputWriter));
    }
};

class AllSPPathsAlgorithm final : public RJAlgorithm {
public:
    AllSPPathsAlgorithm() = default;
    AllSPPathsAlgorithm(const AllSPPathsAlgorithm& other) : RJAlgorithm{other} {}

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
        return std::make_unique<AllSPPathsAlgorithm>(*this);
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
            std::make_unique<AllSPPathsEdgeCompute>(frontierPair.get(), bfsGraph.get());
        auto auxiliaryState = std::make_unique<PathAuxiliaryState>(std::move(bfsGraph));
        auto gdsState = std::make_unique<GDSComputeState>(std::move(frontierPair),
            std::move(edgeCompute), std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        return RJCompState(std::move(gdsState), std::move(outputWriter));
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
