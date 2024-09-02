#include "common/exception/runtime.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "function/gds_function.h"
#include "processor/operator/gds_call.h"
#include "processor/result/factorized_table.h"

// TODO(Semih): Remove
#include <iostream>
using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

class SinglePaths {
public:
    explicit SinglePaths(
        std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes,
        MemoryManager* mm) {
        for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
            // Note: We should be storing nodeID_t atomically but that is not possible. Therefore we
            // store two atomic<uint64_t> values per nodeID. First is the node offset and the second
            // is the tableID.
            parentEdges.insert({tableID, mm->allocateBuffer(false, numNodes * (2*sizeof(std::atomic<uint64_t>)))});
        }
    }

    nodeID_t getParent(nodeID_t nodeID) {
        auto offsetPos = nodeID.offset << 1;
        auto tableIDPos = offsetPos + 1;
        auto bufPtr = reinterpret_cast<std::atomic<uint64_t>*>(
            parentEdges.at(nodeID.tableID).get()->buffer.data());
        return nodeID_t(bufPtr[offsetPos].load(std::memory_order_relaxed),
            bufPtr[tableIDPos].load(std::memory_order_relaxed));
    }

    void setParentEdge(nodeID_t nodeID, nodeID_t parentEdge) {
        KU_ASSERT(currentFixedParentEdges.load(std::memory_order_relaxed) != nullptr);
        // Note: We are changing 2 values non-atomically without any locking or atomics. This
        // should be fine because we assume that all extensions that are being done in a
        // particular iteration happen from the same relationship table, so if there are multiple
        // writes to update the parentEdge for nodeID, say parentEdge1 and parentEdge2, they will have
        // the same tableID. Further they can write different offsets but that's fine because
        // we are keeping track of only 1 singlePath so regardless of which one writes first
        // is fine.
        // Warning: However, this logic has to change when we write both a relID and the offset of
        // the parent. That is because then we need to ensure that both relID and nodeOffset is
        // written atomically.
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

// TODO(Semih): Consider refactoring to two classes, one for DESTINATION_NODES and LENGTHS and the
// other for PATHS. The code will be more explicit that way.
struct SingleSPOutputs : public RJOutputs {
    std::unique_ptr<PathLengths> pathLengths;
    std::unique_ptr<SinglePaths> singlePaths;
    explicit SingleSPOutputs(graph::Graph* graph, nodeID_t sourceNodeID, RJOutputType outputType, MemoryManager* mm = nullptr)
        : RJOutputs(sourceNodeID, outputType) {
        std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes;
        for (common::table_id_t tableID : graph->getNodeTableIDs()) {
            auto numNodes = graph->getNumNodes(tableID);
            nodeTableIDAndNumNodes.push_back({tableID, numNodes});
        }
        pathLengths = std::make_unique<PathLengths>(nodeTableIDAndNumNodes, mm);
        if (RJOutputType::PATHS == outputType) {
            singlePaths = std::make_unique<SinglePaths>(nodeTableIDAndNumNodes, mm);
            // Unlike pathLengths we do not fix the nodeTable of singlePath with
            // sourceNodeID.tableID because when extending a RelTable R(srcNodeLabel, dstNodeLabel),
            // singlePaths should be fixed to dstNodeLabel. This should be done during frontier
            // extensions.
        }
    }

    void beginFrontierComputeBetweenTables(table_id_t,
        table_id_t nextFrontierTableID) override {
        // Note: We do not fix the node table for pathLengths, because Pathlengths is a
        // frontiers implementation and RJCompState will call beginFrontierComputeBetweenTables
        // on Frontiers (and RJOutputs).
        if (RJOutputType::PATHS == outputType) {
            singlePaths->fixNodeTable(nextFrontierTableID);
        }
    };
};

class SingleSPOutputWriter : public RJOutputWriter {
public:
    explicit SingleSPOutputWriter(main::ClientContext* context, RJOutputType outputType)
        : RJOutputWriter(context, outputType) {}
    void materialize(graph::Graph* graph, RJOutputs* rjOutputs,
        processor::FactorizedTable& fTable) const override {
        auto spOutputs = rjOutputs->ptrCast<SingleSPOutputs>();
        srcNodeIDVector->setValue<nodeID_t>(0, spOutputs->sourceNodeID);
        for (auto tableID : graph->getNodeTableIDs()) {
            spOutputs->pathLengths->fixCurFrontierNodeTable(tableID);
            for (offset_t dstNodeOffset = 0;   // dstNodeOffset < 5; // TODO(Semih): Remove
                 dstNodeOffset < spOutputs->pathLengths->getNumNodesInCurFrontierFixedNodeTable();
                 ++dstNodeOffset) {
                auto length =
                    spOutputs->pathLengths->getMaskValueFromCurFrontierFixedMask(dstNodeOffset);
                if (length == PathLengths::UNVISITED) {
                    continue;
                }
                auto dstNodeID = nodeID_t{dstNodeOffset, tableID};
                dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
                if (RJOutputType::LENGTHS == rjOutputType || RJOutputType::PATHS == rjOutputType) {
                    lengthVector->setValue<int64_t>(0, length);
                    if (RJOutputType::PATHS == rjOutputType) {
                        PathVectorWriter writer(pathNodeIDsVector.get());
                        writer.beginWritingNewPath(length);
                        nodeID_t curIntNode = dstNodeID;
                        while (writer.curIntNbrIndex > 0) {
                            curIntNode = spOutputs->singlePaths->getParent(curIntNode);
                            writer.addNewNodeID(curIntNode);
                        }
                    }
                }
                fTable.append(vectors);
            }
        }
    }
};

struct SingleSPLengthsFrontierCompute : public FrontierCompute {
    PathLengthsFrontiers* pathLengthsFrontiers;
    explicit SingleSPLengthsFrontierCompute(PathLengthsFrontiers* pathLengthsFrontiers)
        : pathLengthsFrontiers{pathLengthsFrontiers} {};

    bool edgeCompute(nodeID_t, nodeID_t nbrID) override {
        return pathLengthsFrontiers->pathLengths->getMaskValueFromNextFrontierFixedMask(
                   nbrID.offset) == PathLengths::UNVISITED;
    }

    std::unique_ptr<FrontierCompute> clone() override {
        return std::make_unique<SingleSPLengthsFrontierCompute>(this->pathLengthsFrontiers);
    }
};

struct SingleSPPathsFrontierCompute : public SingleSPLengthsFrontierCompute {
    SinglePaths* singlePaths;
    explicit SingleSPPathsFrontierCompute(PathLengthsFrontiers* pathLengthsFrontiers,
    SinglePaths* singlePaths) :
      SingleSPLengthsFrontierCompute(pathLengthsFrontiers), singlePaths{singlePaths} {};

    bool edgeCompute(nodeID_t curNodeID, nodeID_t nbrID) override {
        auto retVal = pathLengthsFrontiers->pathLengths->getMaskValueFromNextFrontierFixedMask(
                          nbrID.offset) == PathLengths::UNVISITED;
        if (retVal) {
            // We set the nbrID's parent edge to curNodeID;
            singlePaths->setParentEdge(nbrID, curNodeID);
        }
        return retVal;
    }

    std::unique_ptr<FrontierCompute> clone() override {
        return std::make_unique<SingleSPPathsFrontierCompute>(this->pathLengthsFrontiers, this->singlePaths);
    }
};

/**
 * Algorithm for parallel single shortest path computation, i.e., assumes Distinct semantics, so
 * one arbitrary shortest path is returned for each destination. If paths are not returned,
 * multiplicities of each destination is ignored (e.g., if there are 3 paths to a destination d,
 * d is returned only one).
 */
class SingleSPAlgorithm final : public RJAlgorithm {

public:
    explicit SingleSPAlgorithm(RJOutputType outputType) : RJAlgorithm(outputType) {};
    SingleSPAlgorithm(const SingleSPAlgorithm& other) : RJAlgorithm(other) {}

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<SingleSPAlgorithm>(*this);
    }

protected:
    void initLocalState(main::ClientContext* context) override {
        outputWriter = std::make_unique<SingleSPOutputWriter>(context, outputType);
    }

    std::unique_ptr<RJCompState> getFrontiersAndFrontiersCompute(processor::ExecutionContext* executionContext,
        nodeID_t sourceNodeID) override {
        auto spOutputs = std::make_unique<SingleSPOutputs>(sharedState->graph.get(), sourceNodeID,
            outputType, executionContext->clientContext->getMemoryManager());
        auto pathLengthsFrontiers =
            std::make_unique<PathLengthsFrontiers>(spOutputs->pathLengths.get(), executionContext->clientContext->getMaxNumThreadForExec());
        switch (outputType) {
        case RJOutputType::DESTINATION_NODES:
        case RJOutputType::LENGTHS: {
            auto frontierCompute = std::make_unique<SingleSPLengthsFrontierCompute>(pathLengthsFrontiers.get());
            return std::make_unique<RJCompState>(move(spOutputs), move(pathLengthsFrontiers), move(frontierCompute));
        }
        case RJOutputType::PATHS: {
            auto frontierCompute =
                std::make_unique<SingleSPPathsFrontierCompute>(pathLengthsFrontiers.get(), spOutputs->singlePaths.get());
            return std::make_unique<RJCompState>(move(spOutputs), move(pathLengthsFrontiers), move(frontierCompute));
        }
        default:
            throw RuntimeException("Unrecognized RJOutputType in "
                                   "SingleSPAlgorithm::getFrontiersAndFrontiersCompute(): " +
                                   std::to_string(static_cast<uint8_t>(outputType)) + ".");
        }
    }
};

function_set SingleSPDestinationsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(
        name, std::make_unique<SingleSPAlgorithm>(RJOutputType::DESTINATION_NODES));
    result.push_back(std::move(function));
    return result;
}

function_set SingleSPLengthsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(
        name, std::make_unique<SingleSPAlgorithm>(RJOutputType::LENGTHS));
    result.push_back(std::move(function));
    return result;
}

function_set SingleSPPathsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(
        name, std::make_unique<SingleSPAlgorithm>(RJOutputType::PATHS));
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
