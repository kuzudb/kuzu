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
            lastEdges.insert({tableID, mm->allocateBuffer(false, numNodes * (sizeof(nodeID_t)))});
        }
    }

    nodeID_t getParent(nodeID_t nodeID) {
        return reinterpret_cast<nodeID_t*>(
            lastEdges.at(nodeID.tableID).get()->buffer.data())[nodeID.offset];
    }

    void setLastEdge(nodeID_t nodeID, nodeID_t lastEdge) {
        KU_ASSERT(currentFixedLastEdges != nullptr);
        // Note: We are changing 2 values non-atomically without any locking or atomics. This
        // should be fine because we assume that all extensions that are being done in a
        // particular iteration happen from the same relationship table, so if there are multiple
        // writes to update the lastEdge for nodeID, say lastEdge1 and lastEdge2, they will have
        // the same tableID. Further they can write different offsets but that's fine because
        // we are keeping track of only 1 singlePath so regardless of which one writes first
        // is fine.
        // Warning: However, this logic has to change when we write both a relID and the offset of
        // the parent. That is because then we need to ensure that both relID and nodeOffset is
        // written atomically.
        currentFixedLastEdges[nodeID.offset] = lastEdge;
    }

    void fixNodeTable(common::table_id_t tableID) {
        KU_ASSERT(lastEdges.contains(tableID));
        currentFixedLastEdges = reinterpret_cast<nodeID_t*>(lastEdges.at(tableID).get()->buffer.data());
    }

private:
    // TODO(Semih): Rename parentPtrs to parentEdges.
    common::table_id_map_t<std::unique_ptr<MemoryBuffer>> lastEdges;
    nodeID_t* currentFixedLastEdges;
};

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
            for (offset_t nodeOffset = 0; // nodeOffset < 5; TODO(Semih): Remove
                   nodeOffset < spOutputs->pathLengths->getNumNodesInCurFrontierFixedNodeTable();
                 ++nodeOffset) {
                auto length =
                    spOutputs->pathLengths->getMaskValueFromCurFrontierFixedMask(nodeOffset);
                if (length == PathLengths::UNVISITED) {
                    continue;
                }
                auto dstNodeID = nodeID_t{nodeOffset, tableID};
                dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
                if (RJOutputType::LENGTHS == rjOutputType || RJOutputType::PATHS == rjOutputType) {
                    lengthVector->setValue<int64_t>(0, length);
                    if (RJOutputType::PATHS == rjOutputType) {
                        nodeID_t curIntNode = dstNodeID;
                        uint64_t curIntNbrIndex = length > 1 ? length - 1 : 0;
                        pathNodeIDsVector->resetAuxiliaryBuffer();
                        auto pathNodeIDsEntry =
                            ListVector::addList(pathNodeIDsVector.get(), curIntNbrIndex);
                        pathNodeIDsVector->setValue(0, pathNodeIDsEntry);
                        auto dataVector = ListVector::getDataVector(pathNodeIDsVector.get());
                        while (curIntNbrIndex > 0) {
                            curIntNode = spOutputs->singlePaths->getParent(curIntNode);
                            dataVector->setValue(
                                pathNodeIDsEntry.offset + curIntNbrIndex - 1, curIntNode);
                            curIntNbrIndex--;
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
            // We set the nbrID's last edge to curNodeID;
            singlePaths->setLastEdge(nbrID, curNodeID);
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
