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
    // Data type that is allocated to max num nodes per node table.
    using multiplicity_entry_t = std::atomic<uint64_t>;

public:
    PathMultiplicities(std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
        storage::MemoryManager* mm) {
        for (auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
            multiplicityArray.allocate(tableID, numNodes, mm);
            // Question to Trevor: Do I need to use atomics? If so, why?
            auto data = multiplicityArray.getData(tableID);
            for (uint64_t i = 0; i < numNodes; ++i) {
                data[i].store(0);
            }
        }
    }

    // Warning: This function should be called in a single threaded phase. That is because
    // it fixes the target node table. This should not be done at a part of the computation when
    // worker threads might be incrementing multiplicity using the incrementTargetMultiplicity
    // function that assumes curTargetMultiplicities is already fixed to something.
    void incrementMultiplicity(common::nodeID_t nodeID, uint64_t multiplicity) {
        fixTargetNodeTable(nodeID.tableID);
        auto curPtr = getCurTargetMultiplicities();
        curPtr[nodeID.offset].fetch_add(multiplicity);
    }

    void incrementTargetMultiplicity(common::offset_t nodeIDOffset, uint64_t multiplicity) {
        getCurTargetMultiplicities()[nodeIDOffset].fetch_add(multiplicity);
    }

    uint64_t getBoundMultiplicity(common::offset_t nodeOffset) {
        return getCurBoundMultiplicities()[nodeOffset].load(std::memory_order_relaxed);
    }

    uint64_t getTargetMultiplicity(common::offset_t nodeOffset) {
        return getCurTargetMultiplicities()[nodeOffset].load(std::memory_order_relaxed);
    }

    void fixBoundNodeTable(common::table_id_t tableID) {
        curBoundMultiplicities.store(multiplicityArray.getData(tableID), std::memory_order_relaxed);
    }

    void fixTargetNodeTable(common::table_id_t tableID) {
        curTargetMultiplicities.store(multiplicityArray.getData(tableID),
            std::memory_order_relaxed);
    }

private:
    multiplicity_entry_t* getCurTargetMultiplicities() {
        auto retVal = curTargetMultiplicities.load(std::memory_order_relaxed);
        KU_ASSERT(retVal != nullptr);
        return retVal;
    }
    multiplicity_entry_t* getCurBoundMultiplicities() {
        auto retVal = curBoundMultiplicities.load(std::memory_order_relaxed);
        KU_ASSERT(retVal != nullptr);
        return retVal;
    }

private:
    ObjectArraysMap<multiplicity_entry_t> multiplicityArray;
    // curTargetMultiplicities is the multiplicities of the current table that will be updated in a
    // particular rel table extension. This is fixed through the fixTargetNodeTable function.
    std::atomic<multiplicity_entry_t*> curTargetMultiplicities;
    // curBoundMultiplicities is the multiplicities of the table "from which" an extension is
    // being made. This is fixed through the fixBoundNodeTable function.
    std::atomic<multiplicity_entry_t*> curBoundMultiplicities;
};

struct AllSPDestinationsOutputs : public SPOutputs {
public:
    AllSPDestinationsOutputs(std::unordered_map<table_id_t, uint64_t> nodeTableIDAndNumNodes,
        nodeID_t sourceNodeID, MemoryManager* mm = nullptr)
        : SPOutputs(nodeTableIDAndNumNodes, sourceNodeID, mm),
          multiplicities{nodeTableIDAndNumNodes, mm} {}

    void initRJFromSource(nodeID_t source) override {
        multiplicities.incrementMultiplicity(source, 1);
    }

    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) override {
        // Note: We do not fix the node table for pathLengths, which is inherited from AllSPOutputs.
        // See the comment in SingleSPOutputs::beginFrontierComputeBetweenTables() for details.
        multiplicities.fixBoundNodeTable(curFrontierTableID);
        multiplicities.fixTargetNodeTable(nextFrontierTableID);
    };

    void beginWritingOutputsForDstNodesInTable(table_id_t tableID) override {
        pathLengths->fixCurFrontierNodeTable(tableID);
        multiplicities.fixTargetNodeTable(tableID);
    }

public:
    PathMultiplicities multiplicities;
};

class AllSPDestinationsOutputWriter : public DestinationsOutputWriter {
public:
    AllSPDestinationsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
        processor::NodeOffsetMaskMap* outputNodeMask)
        : DestinationsOutputWriter{context, rjOutputs, outputNodeMask} {}

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<AllSPDestinationsOutputWriter>(context, rjOutputs, outputNodeMask);
    }

protected:
    void writeMoreAndAppend(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        uint16_t) const override {
        auto multiplicity =
            rjOutputs->ptrCast<AllSPDestinationsOutputs>()->multiplicities.getTargetMultiplicity(
                dstNodeID.offset);
        for (uint64_t i = 0; i < multiplicity; ++i) {
            fTable.append(vectors);
        }
    }
};

class AllSPDestinationsEdgeCompute : public SPEdgeCompute {
public:
    AllSPDestinationsEdgeCompute(SinglePathLengthsFrontierPair* frontierPair,
        PathMultiplicities* multiplicities)
        : SPEdgeCompute{frontierPair}, multiplicities{multiplicities} {};

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, GraphScanState::Chunk& resultChunk,
        bool) override {
        std::vector<nodeID_t> activeNodes;
        resultChunk.forEach([&](auto nbrNodeID, auto /*edgeID*/) {
            auto nbrVal =
                frontierPair->pathLengths->getMaskValueFromNextFrontierFixedMask(nbrNodeID.offset);
            // We should update the nbrID's multiplicity in 2 cases: 1) if nbrID is being visited
            // for the first time, i.e., when its value in the pathLengths frontier is
            // PathLengths::UNVISITED. Or 2) if nbrID has already been visited but in this
            // iteration, so it's value is curIter + 1.
            auto shouldUpdate = nbrVal == PathLengths::UNVISITED ||
                                nbrVal == frontierPair->pathLengths->getCurIter();
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
    PathMultiplicities* multiplicities;
};

class AllSPPathsEdgeCompute : public SPEdgeCompute {
public:
    AllSPPathsEdgeCompute(SinglePathLengthsFrontierPair* frontiersPair, BFSGraph* bfsGraph)
        : SPEdgeCompute{frontiersPair}, bfsGraph{bfsGraph} {
        parentListBlock = bfsGraph->addNewBlock();
    }

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, GraphScanState::Chunk& resultChunk,
        bool fwdEdge) override {
        std::vector<nodeID_t> activeNodes;
        resultChunk.forEach([&](auto nbrNodeID, auto edgeID) {
            auto nbrLen =
                frontierPair->pathLengths->getMaskValueFromNextFrontierFixedMask(nbrNodeID.offset);
            // We should update the nbrID's multiplicity in 2 cases: 1) if nbrID is being visited
            // for the first time, i.e., when its value in the pathLengths frontier is
            // PathLengths::UNVISITED. Or 2) if nbrID has already been visited but in this
            // iteration, so it's value is curIter + 1.
            auto shouldUpdate = nbrLen == PathLengths::UNVISITED ||
                                nbrLen == frontierPair->pathLengths->getCurIter();
            if (shouldUpdate) {
                if (!parentListBlock->hasSpace()) {
                    parentListBlock = bfsGraph->addNewBlock();
                }
                bfsGraph->addParent(frontierPair->curIter.load(std::memory_order_relaxed),
                    parentListBlock, nbrNodeID /* child */, boundNodeID /* parent */, edgeID,
                    fwdEdge);
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

    expression_vector getResultColumns(Binder*) const override { return getBaseResultColumns(); }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<AllSPDestinationsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto output = std::make_unique<AllSPDestinationsOutputs>(
            sharedState->graph->getNumNodesMap(clientContext->getTx()), sourceNodeID,
            clientContext->getMemoryManager());
        auto outputWriter = std::make_unique<AllSPDestinationsOutputWriter>(clientContext,
            output.get(), sharedState->getOutputNodeMaskMap());
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(output->pathLengths,
            clientContext->getMaxNumThreadForExec());
        auto edgeCompute = std::make_unique<AllSPDestinationsEdgeCompute>(frontierPair.get(),
            &output->multiplicities);
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
    }
};

class AllSPPathsAlgorithm final : public SPAlgorithm {
public:
    AllSPPathsAlgorithm() = default;
    AllSPPathsAlgorithm(const AllSPPathsAlgorithm& other) : SPAlgorithm{other} {}

    expression_vector getResultColumns(Binder*) const override {
        auto columns = getBaseResultColumns();
        auto rjBindData = bindData->ptrCast<RJBindData>();
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
        auto output = std::make_unique<PathsOutputs>(
            sharedState->graph->getNumNodesMap(clientContext->getTx()), sourceNodeID,
            clientContext->getMemoryManager());
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto writerInfo = rjBindData->getPathWriterInfo();
        writerInfo.pathNodeMask = sharedState->getPathNodeMaskMap();
        auto outputWriter = std::make_unique<SPPathsOutputWriter>(clientContext, output.get(),
            sharedState->getOutputNodeMaskMap(), std::move(writerInfo));
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(output->pathLengths,
            clientContext->getMaxNumThreadForExec());
        auto edgeCompute =
            std::make_unique<AllSPPathsEdgeCompute>(frontierPair.get(), &output->bfsGraph);
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
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
