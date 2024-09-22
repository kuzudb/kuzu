#include "common/exception/runtime.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_function_collection.h"
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

class SinglePaths {
public:
    explicit SinglePaths(std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
        MemoryManager* mm) {
        for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
            // Note: We should be storing common::nodeID_t atomically but that is not possible
            // because common::nodeID_t consists of 2 primitive values, an offset and a tableID.
            // Therefore we store two atomic<uint64_t> values per nodeID. First is the node offset
            // and the second is the tableID.
            parentEdges.insert({tableID,
                mm->allocateBuffer(false, numNodes * (2 * sizeof(std::atomic<uint64_t>)))});
        }
    }

    common::nodeID_t getParent(common::nodeID_t nodeID) {
        auto offsetPos = nodeID.offset << 1;
        auto tableIDPos = offsetPos + 1;
        auto bufPtr = reinterpret_cast<std::atomic<uint64_t>*>(
            parentEdges.at(nodeID.tableID).get()->buffer.data());
        return common::nodeID_t(bufPtr[offsetPos].load(std::memory_order_relaxed),
            bufPtr[tableIDPos].load(std::memory_order_relaxed));
    }

    void setParentEdge(common::nodeID_t nodeID, common::nodeID_t parentEdge) {
        KU_ASSERT(currentFixedParentEdges.load(std::memory_order_relaxed) != nullptr);
        // Note: We are changing 2 values non-atomically without any locking or atomics. This
        // should be fine because we assume that all extensions that are being done in a
        // particular iteration happen from the same relationship table, so if there are multiple
        // writes to update the parentEdge for nodeID, say parentEdge1 and parentEdge2, they will
        // have the same tableID. Further they can write different offsets but that's fine because
        // we are keeping track of only 1 singlePath so regardless of which one writes first
        // is fine.
        // Warning: However, this logic has to change when we write both a relID and the offset of
        // the parent. That is because then we need to ensure that both relID and nodeOffset are
        // written atomically. Then we should change this implementation to also use
        // ParentIterAndNextPtr pointers as done with AllShortestPaths and VariableLength joins
        // instead of this more optimized implementation (and *NOT* use software locks).
        auto offsetPos = nodeID.offset << 1;
        auto tableIDPos = offsetPos + 1;
        auto bufPtr = currentFixedParentEdges.load(std::memory_order_relaxed);
        bufPtr[offsetPos].store(parentEdge.offset, std::memory_order_relaxed);
        bufPtr[tableIDPos].store(parentEdge.tableID, std::memory_order_relaxed);
    }

    void fixNodeTable(common::table_id_t tableID) {
        KU_ASSERT(parentEdges.contains(tableID));
        currentFixedParentEdges.store(
            reinterpret_cast<std::atomic<uint64_t>*>(parentEdges.at(tableID).get()->buffer.data()),
            std::memory_order_relaxed);
    }

private:
    common::table_id_map_t<std::unique_ptr<MemoryBuffer>> parentEdges;
    std::atomic<std::atomic<uint64_t>*> currentFixedParentEdges;
};

struct SingleSPOutputs : public SPOutputs {
    explicit SingleSPOutputs(
        std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
        common::nodeID_t sourceNodeID, MemoryManager* mm = nullptr)
        : SPOutputs(nodeTableIDAndNumNodes, sourceNodeID, mm) {}
    // Note: We do not fix the node table for pathLengths, because PathLengths is a
    // FrontierPair implementation and RJCompState will call beginFrontierComputeBetweenTables
    // on FrontierPair (and RJOutputs). That's why this function is empty.
    void beginFrontierComputeBetweenTables(table_id_t, table_id_t) override{};

    void beginWritingOutputsForDstNodesInTable(table_id_t tableID) override {
        pathLengths->fixCurFrontierNodeTable(tableID);
    }
};

struct SingleSPOutputsPaths : public SPOutputs {
    std::unique_ptr<SinglePaths> singlePaths;
    explicit SingleSPOutputsPaths(
        std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
        common::nodeID_t sourceNodeID, MemoryManager* mm)
        : SPOutputs(nodeTableIDAndNumNodes, sourceNodeID, mm) {
        singlePaths = std::make_unique<SinglePaths>(nodeTableIDAndNumNodes, mm);
    }

    void beginFrontierComputeBetweenTables(table_id_t, table_id_t nextFrontierTableID) override {
        singlePaths->fixNodeTable(nextFrontierTableID);
    };

    void beginWritingOutputsForDstNodesInTable(table_id_t tableID) override {
        pathLengths->fixCurFrontierNodeTable(tableID);
        singlePaths->fixNodeTable(tableID);
    }
};

class SingleSPOutputWriterLengths : public SPOutputWriterDsts {
public:
    explicit SingleSPOutputWriterLengths(main::ClientContext* context, RJOutputs* rjOutputs)
        : SPOutputWriterDsts(context, rjOutputs) {
        lengthVector =
            std::make_unique<ValueVector>(LogicalType::INT64(), context->getMemoryManager());
        lengthVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(lengthVector.get());
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<SingleSPOutputWriterLengths>(context, rjOutputs);
    }

protected:
    void writeMoreAndAppend(processor::FactorizedTable& fTable, common::nodeID_t,
        uint16_t length) const override {
        lengthVector->setValue<int64_t>(0, length);
        fTable.append(vectors);
    }

protected:
    std::unique_ptr<common::ValueVector> lengthVector;
};

class SingleSPOutputWriterPaths : public SingleSPOutputWriterLengths {
public:
    explicit SingleSPOutputWriterPaths(main::ClientContext* context, RJOutputs* rjOutputs)
        : SingleSPOutputWriterLengths(context, rjOutputs) {
        pathNodeIDsVector = std::make_unique<ValueVector>(
            LogicalType::LIST(LogicalType::INTERNAL_ID()), context->getMemoryManager());
        pathNodeIDsVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(pathNodeIDsVector.get());
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<SingleSPOutputWriterPaths>(context, rjOutputs);
    }

protected:
    void writeMoreAndAppend(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        uint16_t length) const override {
        lengthVector->setValue<int64_t>(0, length);
        PathVectorWriter writer(pathNodeIDsVector.get());
        writer.beginWritingNewPath(length);
        common::nodeID_t curIntNode = dstNodeID;
        while (writer.nextPathPos > 0) {
            curIntNode =
                rjOutputs->ptrCast<SingleSPOutputsPaths>()->singlePaths->getParent(curIntNode);
            writer.addNewNodeID(curIntNode);
        }
        fTable.append(vectors);
    }

private:
    std::unique_ptr<common::ValueVector> pathNodeIDsVector;
};

struct SingleSPLengthsEdgeCompute : public EdgeCompute {
    SinglePathLengthsFrontierPair* pathLengthsFrontiers;
    explicit SingleSPLengthsEdgeCompute(SinglePathLengthsFrontierPair* pathLengthsFrontiers)
        : pathLengthsFrontiers{pathLengthsFrontiers} {};

    bool edgeCompute(common::nodeID_t, common::nodeID_t nbrID) override {
        return pathLengthsFrontiers->pathLengths->getMaskValueFromNextFrontierFixedMask(
                   nbrID.offset) == PathLengths::UNVISITED;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<SingleSPLengthsEdgeCompute>(this->pathLengthsFrontiers);
    }
};

struct SingleSPPathsEdgeCompute : public SingleSPLengthsEdgeCompute {
    SinglePaths* singlePaths;
    explicit SingleSPPathsEdgeCompute(SinglePathLengthsFrontierPair* pathLengthsFrontiers,
        SinglePaths* singlePaths)
        : SingleSPLengthsEdgeCompute(pathLengthsFrontiers), singlePaths{singlePaths} {};

    bool edgeCompute(common::nodeID_t curNodeID, common::nodeID_t nbrID) override {
        auto retVal = pathLengthsFrontiers->pathLengths->getMaskValueFromNextFrontierFixedMask(
                          nbrID.offset) == PathLengths::UNVISITED;
        if (retVal) {
            // We set the nbrID's parent edge to curNodeID;
            singlePaths->setParentEdge(nbrID, curNodeID);
        }
        return retVal;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<SingleSPPathsEdgeCompute>(this->pathLengthsFrontiers,
            this->singlePaths);
    }
};

/**
 * Algorithm for parallel single shortest path computation, i.e., assumes Distinct semantics, so
 * one arbitrary shortest path is returned for each destination. If paths are not returned,
 * multiplicities of each destination is ignored (e.g., if there are 3 paths to a destination d,
 * d is returned only once).
 */
class SingleSPAlgorithm final : public RJAlgorithm {

public:
    explicit SingleSPAlgorithm(RJOutputType outputType) : RJAlgorithm(outputType){};
    SingleSPAlgorithm(const SingleSPAlgorithm& other) : RJAlgorithm(other) {}

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<SingleSPAlgorithm>(*this);
    }

protected:
    RJCompState getRJCompState(processor::ExecutionContext* executionContext,
        common::nodeID_t sourceNodeID) override {
        std::unique_ptr<SPOutputs> spOutputs;
        std::unique_ptr<RJOutputWriter> outputWriter;
        switch (outputType) {
        case RJOutputType::DESTINATION_NODES:
            spOutputs =
                std::make_unique<SingleSPOutputs>(sharedState->graph->getNodeTableIDAndNumNodes(),
                    sourceNodeID, executionContext->clientContext->getMemoryManager());
            outputWriter = std::make_unique<SPOutputWriterDsts>(executionContext->clientContext,
                spOutputs.get());
            break;
        case RJOutputType::LENGTHS: {
            spOutputs =
                std::make_unique<SingleSPOutputs>(sharedState->graph->getNodeTableIDAndNumNodes(),
                    sourceNodeID, executionContext->clientContext->getMemoryManager());
            outputWriter = std::make_unique<SingleSPOutputWriterLengths>(
                executionContext->clientContext, spOutputs.get());
            break;
        }
        case RJOutputType::PATHS: {
            spOutputs = std::make_unique<SingleSPOutputsPaths>(
                sharedState->graph->getNodeTableIDAndNumNodes(), sourceNodeID,
                executionContext->clientContext->getMemoryManager());
            outputWriter = std::make_unique<SingleSPOutputWriterPaths>(
                executionContext->clientContext, spOutputs.get());
            break;
        }
        default:
            throw RuntimeException(
                "Unrecognized RJOutputType in "
                "SingleSPAlgorithm::getRJCompState() setting output and outputWriter: " +
                std::to_string(static_cast<uint8_t>(outputType)) + ".");
        }
        auto pathLengthsFrontiers = std::make_unique<SinglePathLengthsFrontierPair>(
            spOutputs->pathLengths, executionContext->clientContext->getMaxNumThreadForExec());
        std::unique_ptr<EdgeCompute> edgeCompute;
        switch (outputType) {
        case RJOutputType::DESTINATION_NODES:
        case RJOutputType::LENGTHS: {
            edgeCompute = std::make_unique<SingleSPLengthsEdgeCompute>(pathLengthsFrontiers.get());
            break;
        }
        case RJOutputType::PATHS: {
            edgeCompute = std::make_unique<SingleSPPathsEdgeCompute>(pathLengthsFrontiers.get(),
                spOutputs->ptrCast<SingleSPOutputsPaths>()->singlePaths.get());
            break;
        }
        default:
            throw RuntimeException("Unrecognized RJOutputType in "
                                   "SingleSPAlgorithm::getRJCompState(): " +
                                   std::to_string(static_cast<uint8_t>(outputType)) + ".");
        }
        return RJCompState(std::move(pathLengthsFrontiers), std::move(edgeCompute),
            std::move(spOutputs), std::move(outputWriter));
    }
};

function_set SingleSPDestinationsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(name,
        std::make_unique<SingleSPAlgorithm>(RJOutputType::DESTINATION_NODES)));
    return result;
}

function_set SingleSPLengthsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(name,
        std::make_unique<SingleSPAlgorithm>(RJOutputType::LENGTHS)));
    return result;
}

function_set SingleSPPathsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(name,
        std::make_unique<SingleSPAlgorithm>(RJOutputType::PATHS)));
    return result;
}

} // namespace function
} // namespace kuzu
