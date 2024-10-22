#include "binder/expression/expression_util.h"
#include "common/data_chunk/sel_vector.h"
#include "function/gds/bfs_graph.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_object_manager.h"
#include "function/gds/rec_joins.h"
#include "function/gds_function.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"

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

struct AllSPMultiplicitiesOutputs : public SPOutputs {
public:
    AllSPMultiplicitiesOutputs(std::unordered_map<table_id_t, uint64_t> nodeTableIDAndNumNodes,
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
    AllSPDestinationsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs)
        : DestinationsOutputWriter(context, rjOutputs) {}

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<AllSPDestinationsOutputWriter>(context, rjOutputs);
    }

protected:
    void writeMoreAndAppend(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        uint16_t) const override {
        auto multiplicity =
            rjOutputs->ptrCast<AllSPMultiplicitiesOutputs>()->multiplicities.getTargetMultiplicity(
                dstNodeID.offset);
        for (uint64_t i = 0; i < multiplicity; ++i) {
            fTable.append(vectors);
        }
    }
};

class AllSPLengthsOutputWriter : public DestinationsOutputWriter {
public:
    AllSPLengthsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs)
        : DestinationsOutputWriter(context, rjOutputs) {
        lengthVector =
            std::make_unique<ValueVector>(LogicalType::INT64(), context->getMemoryManager());
        lengthVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(lengthVector.get());
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<AllSPLengthsOutputWriter>(context, rjOutputs);
    }

protected:
    void writeMoreAndAppend(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        uint16_t length) const override {
        lengthVector->setValue<int64_t>(0, length);
        auto multiplicity =
            rjOutputs->ptrCast<AllSPMultiplicitiesOutputs>()->multiplicities.getTargetMultiplicity(
                dstNodeID.offset);
        for (uint64_t i = 0; i < multiplicity; ++i) {
            fTable.append(vectors);
        }
    }

private:
    std::unique_ptr<common::ValueVector> lengthVector;
};

class VarLenPathsOutputWriter : public PathsOutputWriter {
public:
    VarLenPathsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
        PathsOutputWriterInfo info)
        : PathsOutputWriter{context, rjOutputs, info} {}

    bool skipWriting(common::nodeID_t dstNodeID) const override {
        auto pathsOutputs = rjOutputs->ptrCast<PathsOutputs>();
        auto firstParent = pathsOutputs->bfsGraph.getCurFixedParentPtrs()[dstNodeID.offset].load(
            std::memory_order_relaxed);
        // For variable lengths joins, we skip a destination node d in the following conditions:
        //    (i) if no path has reached d from the source, except when the lower bound is 0.
        //    (ii) the longest path that has reached d, which is stored in the iter value of the
        //    firstParent is smaller than the lower bound.
        // We also do not output any results if a destination node has not been reached.
        if (nullptr == firstParent) {
            return info.lowerBound > 0;
        } else {
            return firstParent->getIter() < info.lowerBound;
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<VarLenPathsOutputWriter>(context, rjOutputs, info);
    }
};

class AllSPLengthsEdgeCompute : public EdgeCompute {
public:
    AllSPLengthsEdgeCompute(SinglePathLengthsFrontierPair* frontierPair,
        PathMultiplicities* multiplicities)
        : frontierPair{frontierPair}, multiplicities{multiplicities} {};

    void edgeCompute(nodeID_t boundNodeID, std::span<const nodeID_t> nbrIDs,
        std::span<const relID_t>, SelectionVector& mask, bool) override {
        size_t activeCount = 0;
        mask.forEach([&](auto i) {
            auto nbrVal =
                frontierPair->pathLengths->getMaskValueFromNextFrontierFixedMask(nbrIDs[i].offset);
            // We should update the nbrID's multiplicity in 2 cases: 1) if nbrID is being visited
            // for the first time, i.e., when its value in the pathLengths frontier is
            // PathLengths::UNVISITED. Or 2) if nbrID has already been visited but in this
            // iteration, so it's value is curIter + 1.
            auto shouldUpdate = nbrVal == PathLengths::UNVISITED ||
                                nbrVal == frontierPair->pathLengths->getCurIter();
            if (shouldUpdate) {
                // Note: This is safe because curNodeID is in the current frontier, so its
                // shortest paths multiplicity is guaranteed to not change in the current iteration.
                multiplicities->incrementTargetMultiplicity(nbrIDs[i].offset,
                    multiplicities->getBoundMultiplicity(boundNodeID.offset));
            }
            if (nbrVal == PathLengths::UNVISITED) {
                mask.getMutableBuffer()[activeCount++] = i;
            }
        });
        mask.setToFiltered(activeCount);
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<AllSPLengthsEdgeCompute>(frontierPair, multiplicities);
    }

private:
    SinglePathLengthsFrontierPair* frontierPair;
    PathMultiplicities* multiplicities;
};

class AllSPPathsEdgeCompute : public EdgeCompute {
public:
    AllSPPathsEdgeCompute(SinglePathLengthsFrontierPair* frontiersPair, BFSGraph* bfsGraph)
        : frontiersPair{frontiersPair}, bfsGraph{bfsGraph} {
        parentListBlock = bfsGraph->addNewBlock();
    }

    void edgeCompute(nodeID_t boundNodeID, std::span<const nodeID_t> nbrNodeIDs,
        std::span<const relID_t> edgeIDs, SelectionVector& mask, bool fwdEdge) override {
        size_t activeCount = 0;
        mask.forEach([&](auto i) {
            auto nbrLen = frontiersPair->pathLengths->getMaskValueFromNextFrontierFixedMask(
                nbrNodeIDs[i].offset);
            // We should update the nbrID's multiplicity in 2 cases: 1) if nbrID is being visited
            // for the first time, i.e., when its value in the pathLengths frontier is
            // PathLengths::UNVISITED. Or 2) if nbrID has already been visited but in this
            // iteration, so it's value is curIter + 1.
            auto shouldUpdate = nbrLen == PathLengths::UNVISITED ||
                                nbrLen == frontiersPair->pathLengths->getCurIter();
            if (shouldUpdate) {
                if (!parentListBlock->hasSpace()) {
                    parentListBlock = bfsGraph->addNewBlock();
                }
                bfsGraph->addParent(frontiersPair->curIter.load(std::memory_order_relaxed),
                    parentListBlock, nbrNodeIDs[i] /* child */, boundNodeID /* parent */,
                    edgeIDs[i], fwdEdge);
            }
            if (nbrLen == PathLengths::UNVISITED) {
                mask.getMutableBuffer()[activeCount++] = i;
            }
        });
        mask.setToFiltered(activeCount);
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<AllSPPathsEdgeCompute>(frontiersPair, bfsGraph);
    }

private:
    SinglePathLengthsFrontierPair* frontiersPair;
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

    expression_vector getResultColumns(Binder* binder) const override {
        return getBaseResultColumns(binder);
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<AllSPDestinationsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto output = std::make_unique<AllSPMultiplicitiesOutputs>(
            sharedState->graph->getNodeTableIDAndNumNodes(), sourceNodeID,
            clientContext->getMemoryManager());
        auto outputWriter =
            std::make_unique<AllSPDestinationsOutputWriter>(clientContext, output.get());
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(output->pathLengths,
            clientContext->getMaxNumThreadForExec());
        auto edgeCompute =
            std::make_unique<AllSPLengthsEdgeCompute>(frontierPair.get(), &output->multiplicities);
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
    }
};

class AllSPLengthsAlgorithm final : public SPAlgorithm {
public:
    AllSPLengthsAlgorithm() = default;
    AllSPLengthsAlgorithm(const AllSPLengthsAlgorithm& other) : SPAlgorithm{other} {}

    expression_vector getResultColumns(Binder* binder) const override {
        auto columns = getBaseResultColumns(binder);
        columns.push_back(getLengthColumn(binder));
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<AllSPLengthsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto output = std::make_unique<AllSPMultiplicitiesOutputs>(
            sharedState->graph->getNodeTableIDAndNumNodes(), sourceNodeID,
            clientContext->getMemoryManager());
        auto outputWriter = std::make_unique<AllSPLengthsOutputWriter>(clientContext, output.get());
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(output->pathLengths,
            clientContext->getMaxNumThreadForExec());
        auto edgeCompute =
            std::make_unique<AllSPLengthsEdgeCompute>(frontierPair.get(), &output->multiplicities);
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
    }
};

class AllSPPathsAlgorithm final : public SPAlgorithm {
public:
    AllSPPathsAlgorithm() = default;
    AllSPPathsAlgorithm(const AllSPPathsAlgorithm& other) : SPAlgorithm{other} {}

    expression_vector getResultColumns(Binder* binder) const override {
        auto columns = getBaseResultColumns(binder);
        columns.push_back(getLengthColumn(binder));
        columns.push_back(getPathNodeIDsColumn(binder));
        columns.push_back(getPathEdgeIDsColumn(binder));
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<AllSPPathsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto output =
            std::make_unique<PathsOutputs>(sharedState->graph->getNodeTableIDAndNumNodes(),
                sourceNodeID, clientContext->getMemoryManager());
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto writerInfo = rjBindData->getPathWriterInfo();
        writerInfo.pathNodeMask = sharedState->getPathNodeMaskMap();
        auto outputWriter = std::make_unique<SPPathsOutputWriter>(clientContext, output.get(),
            std::move(writerInfo));
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(output->pathLengths,
            clientContext->getMaxNumThreadForExec());
        auto edgeCompute =
            std::make_unique<AllSPPathsEdgeCompute>(frontierPair.get(), &output->bfsGraph);
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
    }
};

struct VarLenJoinsEdgeCompute : public EdgeCompute {
    DoublePathLengthsFrontierPair* frontierPair;
    BFSGraph* bfsGraph;
    ObjectBlock<ParentList>* parentPtrsBlock = nullptr;

    VarLenJoinsEdgeCompute(DoublePathLengthsFrontierPair* frontierPair, BFSGraph* bfsGraph)
        : frontierPair{frontierPair}, bfsGraph{bfsGraph} {
        parentPtrsBlock = bfsGraph->addNewBlock();
    };

    void edgeCompute(nodeID_t boundNodeID, std::span<const nodeID_t> nbrNodeIDs,
        std::span<const relID_t> edgeIDs, SelectionVector& mask, bool isFwd) override {
        mask.forEach([&](auto i) {
            // We should always update the nbrID in variable length joins
            if (!parentPtrsBlock->hasSpace()) {
                parentPtrsBlock = bfsGraph->addNewBlock();
            }
            bfsGraph->addParent(frontierPair->getCurrentIter(), parentPtrsBlock,
                nbrNodeIDs[i] /* child */, boundNodeID /* parent */, edgeIDs[i], isFwd);
            // all nodes visited are active, so the mask is left unmodified
        });
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<VarLenJoinsEdgeCompute>(frontierPair, bfsGraph);
    }
};

/**
 * Algorithm for parallel all shortest paths computation, so all shortest paths from a source to
 * is returned for each destination. If paths are not returned, multiplicities indicate the
 * number of paths to each destination.
 */
class VarLenJoinsAlgorithm final : public RJAlgorithm {
public:
    VarLenJoinsAlgorithm() = default;
    VarLenJoinsAlgorithm(const VarLenJoinsAlgorithm& other) : RJAlgorithm(other) {}

    /*
     * Inputs include the following:
     *
     * graph::ANY
     * srcNode::NODE
     * lowerBound::INT64
     * upperBound::INT64
     * direction::STRING
     */
    std::vector<LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::NODE, LogicalTypeID::INT64, LogicalTypeID::INT64,
            LogicalTypeID::STRING};
    }

    void bind(const expression_vector& params, Binder* binder,
        graph::GraphEntry& graphEntry) override {
        auto nodeInput = params[1];
        auto nodeOutput = bindNodeOutput(binder, graphEntry);
        auto lowerBound = ExpressionUtil::getLiteralValue<int64_t>(*params[2]);
        auto upperBound = ExpressionUtil::getLiteralValue<int64_t>(*params[3]);
        validateLowerUpperBound(lowerBound, upperBound);
        auto extendDirection = ExtendDirectionUtil::fromString(
            ExpressionUtil::getLiteralValue<std::string>(*params[4]));
        bindData = std::make_unique<RJBindData>(nodeInput, nodeOutput, lowerBound, upperBound,
            PathSemantic::WALK, extendDirection);
    }

    binder::expression_vector getResultColumns(Binder* binder) const override {
        auto columns = getBaseResultColumns(binder);
        columns.push_back(getLengthColumn(binder));
        columns.push_back(getPathNodeIDsColumn(binder));
        columns.push_back(getPathEdgeIDsColumn(binder));
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<VarLenJoinsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto mm = clientContext->getMemoryManager();
        auto nodeTableToNumNodes = sharedState->graph->getNodeTableIDAndNumNodes();
        auto output = std::make_unique<PathsOutputs>(nodeTableToNumNodes, sourceNodeID, mm);
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto writerInfo = rjBindData->getPathWriterInfo();
        writerInfo.pathNodeMask = sharedState->getPathNodeMaskMap();
        auto outputWriter = std::make_unique<VarLenPathsOutputWriter>(clientContext, output.get(),
            std::move(writerInfo));
        auto frontierPair = std::make_unique<DoublePathLengthsFrontierPair>(nodeTableToNumNodes,
            clientContext->getMaxNumThreadForExec(), mm);
        auto edgeCompute =
            std::make_unique<VarLenJoinsEdgeCompute>(frontierPair.get(), &output->bfsGraph);
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
    }
};

function_set VarLenJoinsFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<VarLenJoinsAlgorithm>();
    result.push_back(
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo)));
    return result;
}

function_set AllSPDestinationsFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<AllSPDestinationsAlgorithm>();
    result.push_back(
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo)));
    return result;
}

function_set AllSPLengthsFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<AllSPLengthsAlgorithm>();
    result.push_back(
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo)));
    return result;
}

function_set AllSPPathsFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<AllSPPathsAlgorithm>();
    result.push_back(
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo)));
    return result;
}

} // namespace function
} // namespace kuzu
