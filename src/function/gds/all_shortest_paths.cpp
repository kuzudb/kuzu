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

struct ParentAndNextPtr {
public:
    ParentAndNextPtr(nodeID_t parent, ParentAndNextPtr* nextParentPtr)
        : parent{parent}, nextParentPtr{nextParentPtr} {}
    nodeID_t parent;
    ParentAndNextPtr* nextParentPtr;
};

// EAch AllPathsBlock should be accessed by a single thread.
class AllPathsBlock {
public:
    AllPathsBlock(std::unique_ptr<MemoryBuffer> lastEdges, uint64_t sizeInBytes)
        : lastEdges{std::move(lastEdges)} {
        maxElements = sizeInBytes / (sizeof(ParentAndNextPtr));
        nextPosToWrite = 0;
        std::cout << "sizeInBytes: " << sizeInBytes
                  << "sizeof(ParentAndNextPtr):" << sizeof(ParentAndNextPtr)
                  << " maxElements: " << maxElements << std::endl;
    }

    bool hasSpace() { return nextPosToWrite < maxElements; }

    // Warning: Make sure hasSpace has returned true already.
    uint8_t* writeParentEdge(nodeID_t parentEdge) {
        auto parentEdgePtr = reinterpret_cast<ParentAndNextPtr*>(lastEdges->buffer.data());
        parentEdgePtr[nextPosToWrite] = ParentAndNextPtr(parentEdge, nullptr);
        return reinterpret_cast<uint8_t*>(&parentEdgePtr[nextPosToWrite++]);
    }

private:
    std::unique_ptr<MemoryBuffer> lastEdges;
    uint64_t maxElements;
    uint64_t nextPosToWrite;
};

class AllPathsAtomics {
    // TODO(Semih/Xiyang): Make this configurable.
    static constexpr uint64_t ALL_PATHS_BLOCK_SIZE = (std::uint64_t)1 << 19;

public:
    explicit AllPathsAtomics(
        std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes,
        MemoryManager* mm)
        : mm{mm} {
        for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
            auto memBuffer = mm->allocateBuffer(false, numNodes * (sizeof(std::atomic<uint8_t*>)));
            auto pointers =
                reinterpret_cast<std::atomic<uint8_t*>*>(memBuffer.get()->buffer.data());
            for (int i = 0; i < numNodes; ++i) {
                // Note: We are using memory_order_relaxed here because we are assuming that
                // this code is running by a master thread which will run a memory barrier
                // before worker thrads start.
                pointers[i].store(nullptr, std::memory_order_relaxed);
            }
            parentPtrs.insert({tableID, move(memBuffer)});
        }
    }

    AllPathsBlock* addNewBlock() {
        std::unique_lock lck{mtx};
        auto memBlock = mm->allocateBuffer(false /* don't init to 0 */, ALL_PATHS_BLOCK_SIZE);
        blocks.push_back(std::make_unique<AllPathsBlock>(move(memBlock), ALL_PATHS_BLOCK_SIZE));
        return blocks[blocks.size() - 1].get();
    }

    nodeID_t getParent(nodeID_t nodeID) {
        return reinterpret_cast<nodeID_t*>(
            parentPtrs.at(nodeID.tableID).get()->buffer.data())[nodeID.offset];
    }

    void addParent(nodeID_t nodeID, ParentAndNextPtr* parentEdgePtr) {
        KU_ASSERT(currFixedParentPtrs != nullptr);
        // Since by default the parentPtr of each node is nullptr, that's what we start with.
        ParentAndNextPtr* expected = nullptr;
        while (!currFixedParentPtrs[nodeID.offset].compare_exchange_strong(expected, parentEdgePtr));
        parentEdgePtr->nextParentPtr = expected;
    }

    void fixNodeTable(common::table_id_t tableID) {
        KU_ASSERT(parentPtrs.contains(tableID));
        currFixedParentPtrs =
            reinterpret_cast<std::atomic<ParentAndNextPtr*>*>(parentPtrs.at(tableID).get()->buffer.data());
    }

private:
    std::mutex mtx;
    MemoryManager* mm;
    common::table_id_map_t<std::unique_ptr<MemoryBuffer>> parentPtrs;
    std::atomic<ParentAndNextPtr*>* currFixedParentPtrs;
    std::vector<std::unique_ptr<AllPathsBlock>> blocks;
};

/**
 * A dense storage structures for multiplicities for multiple node tables.
 */
class PathMultiplicities {
public:
    explicit PathMultiplicities(
        std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes,
        MemoryManager* mm) {
        for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
            auto memoryBuffer = mm->allocateBuffer(false, numNodes * sizeof(std::atomic<uint64_t>));
            // Question to Trevor: Do I need to use atomics? If so, why?
            auto multiplicitiesPtr =
                reinterpret_cast<std::atomic<uint64_t>*>(memoryBuffer->buffer.data());
            for (uint64_t i = 0; i < numNodes; ++i) {
                multiplicitiesPtr[i].store(0);
            }
            multiplicitiesMap.insert({tableID, move(memoryBuffer)});
        }
    }

    // Warning: This function should be called in a single threaded phase. That is because
    // it fixes the node table, which should not be done at a part of the computation when
    // worker threads might be incrementing multiplicity using the incrementMultiplicity function
    // that assumes currMultiplicities is already fixed to something.
    void incrementMultiplicity(nodeID_t nodeID, uint64_t multiplicity) {
        fixNodeTable(nodeID.tableID);
        currMultiplicities[nodeID.offset].fetch_add(multiplicity);
    }

    void incrementMultiplicity(offset_t nodeIDOffset, uint64_t multiplicity) {
        KU_ASSERT(currMultiplicities != nullptr);
        currMultiplicities[nodeIDOffset].fetch_add(multiplicity);
    }

    uint64_t getMultiplicity(offset_t nodeOffset) {
        KU_ASSERT(currMultiplicities != nullptr);
        return currMultiplicities[nodeOffset].load();
    }

    void fixNodeTable(common::table_id_t tableID) {
        KU_ASSERT(multiplicitiesMap.contains(tableID));
        currMultiplicities = reinterpret_cast<std::atomic<uint64_t>*>(
            multiplicitiesMap.at(tableID).get()->buffer.data());
    }

private:
    common::table_id_map_t<std::unique_ptr<MemoryBuffer>> multiplicitiesMap;
    // currMultiplicities is the multiplicities of the current table that will be updated in a
    // particular rel table extension. This is fixed through the fixNodeTable function.
    std::atomic<uint64_t>* currMultiplicities;
};

/**
 * A data structure to keep track of all (shortest) path from one source node to
 * multiple destination (and intermediate) nodes as "backward BFS graph" form. The data structure is
 * "dense" in the sense that it keeps space for a pointer to the "backwards edges" for each
 * possible node that can be in the destination. Supports multi-label nodes.
 */
class AllPaths {
public:
    explicit AllPaths(std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes,
        MemoryManager* mm) {
        for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
            lastEdges.insert({tableID, mm->allocateBuffer(false, numNodes * sizeof(nodeID_t))});
        }
    }

    void setLastEdge(nodeID_t nodeID, nodeID_t lastEdge) {
        KU_ASSERT(currentFixedLastEdges != nullptr);
        currentFixedLastEdges[nodeID.offset] = lastEdge;
    }

    nodeID_t getParent(nodeID_t nodeID) {
        return reinterpret_cast<nodeID_t*>(
            lastEdges.at(nodeID.tableID).get()->buffer.data())[nodeID.offset];
    }

    void fixNodeTable(common::table_id_t tableID) {
        KU_ASSERT(lastEdges.contains(tableID));
        currentFixedLastEdges =
            reinterpret_cast<nodeID_t*>(lastEdges.at(tableID).get()->buffer.data());
    }

private:
    common::table_id_map_t<std::unique_ptr<MemoryBuffer>> lastEdges;
    nodeID_t* currentFixedLastEdges;
};

class AllSPPathsFrontiers : public PathLengthsFrontiers {
    friend struct AllSPPathsFrontierCompute;

public:
    explicit AllSPPathsFrontiers(PathLengths* pathLengths, AllPaths* allPaths,
        uint64_t maxThreadsForExec)
        : PathLengthsFrontiers(pathLengths, maxThreadsForExec), allPaths{allPaths} {}
    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) override {
        PathLengthsFrontiers::beginFrontierComputeBetweenTables(curFrontierTableID,
            nextFrontierTableID);
        allPaths->fixNodeTable(nextFrontierTableID);
    }

private:
    AllPaths* allPaths;
};

struct AllSPOutputs : public RJOutputs {
    std::unique_ptr<PathLengths> pathLengths;
    // Multiplicities is only used the output type is DESTINATION_NODES or LENGTHS.
    std::unique_ptr<PathMultiplicities> multiplicities;

    explicit AllSPOutputs(graph::Graph* graph, nodeID_t sourceNodeID, RJOutputType outputType,
        MemoryManager* mm = nullptr)
        : RJOutputs(sourceNodeID, outputType) {
        std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes;
        for (common::table_id_t tableID : graph->getNodeTableIDs()) {
            auto numNodes = graph->getNumNodes(tableID);
            nodeTableIDAndNumNodes.push_back({tableID, numNodes});
        }
        pathLengths = std::make_unique<PathLengths>(nodeTableIDAndNumNodes, mm);
        multiplicities = std::make_unique<PathMultiplicities>(nodeTableIDAndNumNodes, mm);
    }

    void initRJFromSource(nodeID_t source) override {
        multiplicities->incrementMultiplicity(source, 1);
    }

    void beginFrontierComputeBetweenTables(table_id_t, table_id_t nextFrontierTableID) override {
        // Note: We do not fix the node table for pathLengths. See the comment in
        // SingleSPOutputs::beginFrontierComputeBetweenTables() for details.
        // Note: We do not need to keep track of multiplicities when we're keeping track of paths,
        // since the number of paths to each destination d will equal the multiplicity of d.
        if (RJOutputType::DESTINATION_NODES == outputType || RJOutputType::LENGTHS == outputType) {
            multiplicities->fixNodeTable(nextFrontierTableID);
        }
    };
};

class AllSPOutputWriter : public RJOutputWriter {
public:
    explicit AllSPOutputWriter(main::ClientContext* context, RJOutputType outputType)
        : RJOutputWriter(context, outputType) {}
    void materialize(graph::Graph* graph, RJOutputs* rjOutputs,
        processor::FactorizedTable& fTable) const override {
        auto spOutputs = rjOutputs->ptrCast<AllSPOutputs>();
        srcNodeIDVector->setValue<nodeID_t>(0, spOutputs->sourceNodeID);
        for (auto tableID : graph->getNodeTableIDs()) {
            spOutputs->multiplicities->fixNodeTable(tableID);
            spOutputs->pathLengths->fixCurFrontierNodeTable(tableID);
            for (offset_t nodeOffset = 0;
                 nodeOffset < spOutputs->pathLengths->getNumNodesInCurFrontierFixedNodeTable();
                 ++nodeOffset) {
                auto length =
                    spOutputs->pathLengths->getMaskValueFromCurFrontierFixedMask(nodeOffset);
                if (length == PathLengths::UNVISITED) {
                    continue;
                }
                for (uint64_t i = 0; i < spOutputs->multiplicities->getMultiplicity(nodeOffset);
                     ++i) {
                    auto dstNodeID = nodeID_t{nodeOffset, tableID};
                    dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
                    if (RJOutputType::LENGTHS == rjOutputType ||
                        RJOutputType::PATHS == rjOutputType) {
                        lengthVector->setValue<int64_t>(0, length);
                    }
                    fTable.append(vectors);
                }
            }
        }
    }
};

struct AllSPLengthsFrontierCompute : public FrontierCompute {
    explicit AllSPLengthsFrontierCompute(PathLengthsFrontiers* pathLengthsFrontiers,
        PathMultiplicities* multiplicities)
        : pathLengthsFrontiers{pathLengthsFrontiers}, multiplicities{multiplicities} {};

    bool edgeCompute(nodeID_t curNodeID, nodeID_t nbrID) override {
        auto nbrVal =
            pathLengthsFrontiers->pathLengths->getMaskValueFromNextFrontierFixedMask(nbrID.offset);
        // We should update the nbrID's multiplicity in 2 cases: 1) if nbrID is being visited for
        // the first time, i.e., when its value in the pathLengths frontier is
        // PathLengths::UNVISITED. Or 2) if nbrID has already been visited but in this iteration,
        // so it's value is curIter + 1.
        auto shouldUpdate = nbrVal == PathLengths::UNVISITED ||
                            nbrVal == pathLengthsFrontiers->pathLengths->curIter + 1;
        if (shouldUpdate) {
            // Note: This is safe because curNodeID is in the current frontier, so its
            // shortest paths multiplicity is guaranteed to not change in the current iteration.
            multiplicities->incrementMultiplicity(nbrID.offset,
                multiplicities->getMultiplicity(curNodeID.offset));
        }
        return nbrVal == PathLengths::UNVISITED;
    }

    void initFrontierExtensions(table_id_t toNodeTableID) override {
        multiplicities->fixNodeTable(toNodeTableID);
    };

    //    void init() override {
    //        fixedTableMultiplicities = multiplicities->getCurrNodeTableMultiplicities();
    //    }

    std::unique_ptr<FrontierCompute> clone() override {
        return std::make_unique<AllSPLengthsFrontierCompute>(this->pathLengthsFrontiers,
            this->multiplicities);
    }

private:
    PathLengthsFrontiers* pathLengthsFrontiers;
    PathMultiplicities* multiplicities;
    //    std::unique_ptr<SingleNodeTableMultiplicities> fixedTableMultiplicities;
};

struct AllSPPathsFrontierCompute : public FrontierCompute {
    AllSPPathsFrontiers* allSPPathsFrontiers;
    explicit AllSPPathsFrontierCompute(AllSPPathsFrontiers* allPathsFrontiers)
        : allSPPathsFrontiers{allPathsFrontiers} {};

    bool edgeCompute(nodeID_t curNodeID, nodeID_t nbrID) override {
        auto retVal = allSPPathsFrontiers->pathLengths->getMaskValueFromNextFrontierFixedMask(
                          nbrID.offset) == PathLengths::UNVISITED;
        if (retVal) {
            // We set the nbrID's last edge to curNodeID;
            allSPPathsFrontiers->allPaths->setLastEdge(nbrID, curNodeID);
        }
        return retVal;
    }
};

/**
 * Algorithm for parallel single shortest path computation, i.e., assumes Distinct semantics, so
 * one arbitrary shortest path is returned for each destination. If paths are not returned,
 * multiplicities of each destination is ignored (e.g., if there are 3 paths to a destination d,
 * d is returned only one).
 */
class AllSPAlgorithm final : public RJAlgorithm {

public:
    explicit AllSPAlgorithm(RJOutputType outputType) : RJAlgorithm(outputType){};
    AllSPAlgorithm(const AllSPAlgorithm& other) : RJAlgorithm(other) {}

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<AllSPAlgorithm>(*this);
    }

protected:
    void initLocalState(main::ClientContext* context) override {
        outputWriter = std::make_unique<AllSPOutputWriter>(context, outputType);
    }

    std::unique_ptr<RJCompState> getFrontiersAndFrontiersCompute(
        processor::ExecutionContext* executionContext, nodeID_t sourceNodeID) override {
        auto spOutputs = std::make_unique<AllSPOutputs>(sharedState->graph.get(), sourceNodeID,
            outputType, executionContext->clientContext->getMemoryManager());
        switch (outputType) {
        case RJOutputType::DESTINATION_NODES:
        case RJOutputType::LENGTHS: {
            auto pathLengthsFrontiers =
                std::make_unique<PathLengthsFrontiers>(spOutputs->pathLengths.get(),
                    executionContext->clientContext->getMaxNumThreadForExec());
            auto frontierCompute = std::make_unique<AllSPLengthsFrontierCompute>(
                pathLengthsFrontiers.get(), spOutputs->multiplicities.get());
            return std::make_unique<RJCompState>(move(spOutputs), move(pathLengthsFrontiers),
                move(frontierCompute));
        }
            //        case RJOutputType::PATHS: {
            //            auto singlePathsFrontiers = std::make_unique<AllSPPathsFrontiers>(
            //                sourceState->pathLengths.get(), sourceState->singlePaths.get());
            //            auto frontierCompute =
            //                std::make_unique<AllSPPathsFrontierCompute>(singlePathsFrontiers.get());
            //            return std::make_unique<RJCompState>(move(sourceState),
            //            move(singlePathsFrontiers), move(frontierCompute));
            //        }
        default:
            throw RuntimeException("Unrecognized RJOutputType in "
                                   "RJAlgorithm::getFrontiersAndFrontiersCompute(): " +
                                   std::to_string(static_cast<uint8_t>(outputType)) + ".");
        }
    }
};

function_set AllSPDestinationsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name,
        std::make_unique<AllSPAlgorithm>(RJOutputType::DESTINATION_NODES));
    result.push_back(std::move(function));
    return result;
}

function_set AllSPLengthsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name,
        std::make_unique<AllSPAlgorithm>(RJOutputType::LENGTHS));
    result.push_back(std::move(function));
    return result;
}

function_set AllSPPathsFunction::getFunctionSet() {
    function_set result;
    auto function =
        std::make_unique<GDSFunction>(name, std::make_unique<AllSPAlgorithm>(RJOutputType::PATHS));
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
