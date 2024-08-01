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
    ParentAndNextPtr(nodeID_t parent) {
        offset.store(parent.offset, std::memory_order_relaxed);
        tableID.store(parent.tableID, std::memory_order_relaxed);
    }

    ParentAndNextPtr& operator=(const ParentAndNextPtr& rhs) {
        offset.store(rhs.offset.load(std::memory_order_relaxed), std::memory_order_relaxed);
        tableID.store(rhs.tableID.load(std::memory_order_relaxed), std::memory_order_relaxed);
        nextPtr.store(rhs.nextPtr.load(std::memory_order_relaxed), std::memory_order_relaxed);
        return *this;
    }

    void setNextPtr(ParentAndNextPtr* nextParentPtr) {
        nextPtr.store(nextParentPtr, std::memory_order_relaxed);
    }

    std::atomic<uint64_t> offset;
    std::atomic<uint64_t> tableID;
    std::atomic<ParentAndNextPtr*> nextPtr;
};

// Each ParentPtrsBlock should be accessed by a single thread.
class ParentPtrsBlock {
    friend class ParentPtrsAtomics;
public:
    ParentPtrsBlock(std::unique_ptr<MemoryBuffer> block, uint64_t sizeInBytes)
        : block{std::move(block)} {
        maxElements.store(sizeInBytes / (sizeof(ParentAndNextPtr)), std::memory_order_relaxed);
        nextPosToWrite.store(0, std::memory_order_relaxed);
        std::cout << "sizeInBytes: " << sizeInBytes
                  << "sizeof(ParentAndNextPtr):" << sizeof(ParentAndNextPtr)
                  << " maxElements: " << maxElements.load(std::memory_order_relaxed) << std::endl;
    }

    bool hasSpace() {
        return nextPosToWrite.load(std::memory_order_relaxed) <
               maxElements.load(std::memory_order_relaxed);
    }

private:
    std::unique_ptr<MemoryBuffer> block;
    std::atomic<uint64_t> maxElements;
    std::atomic<uint64_t> nextPosToWrite;
};

class ParentPtrsAtomics {
    // TODO(Semih/Xiyang): Make this configurable.
    static constexpr uint64_t ALL_PATHS_BLOCK_SIZE = (std::uint64_t)1 << 19;

public:
    explicit ParentPtrsAtomics(
        std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes,
        MemoryManager* mm)
        : mm{mm} {
        for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
            auto memBuffer = mm->allocateBuffer(false, numNodes * (sizeof(std::atomic<ParentAndNextPtr*>)));
            auto pointers =
                reinterpret_cast<std::atomic<uint8_t*>*>(memBuffer.get()->buffer.data());
            for (uint64_t i = 0; i < numNodes; ++i) {
                // Note: We are using memory_order_relaxed here because we are assuming that
                // this code is running by a master thread which will run a memory barrier
                // before worker thrads start.
                pointers[i].store(nullptr, std::memory_order_relaxed);
            }
            parentPtrs.insert({tableID, move(memBuffer)});
        }
    }

    ParentPtrsBlock* addNewBlock() {
        std::unique_lock lck{mtx};
        auto memBlock = mm->allocateBuffer(false /* don't init to 0 */, ALL_PATHS_BLOCK_SIZE);
        blocks.push_back(std::make_unique<ParentPtrsBlock>(move(memBlock), ALL_PATHS_BLOCK_SIZE));
        return blocks[blocks.size() - 1].get();
    }

    ParentAndNextPtr* getInitialParentAndNextPtr(nodeID_t nodeID) {
        return reinterpret_cast<std::atomic<ParentAndNextPtr*>*>(
            parentPtrs.at(nodeID.tableID).get()->buffer.data())[nodeID.offset].load(std::memory_order_relaxed);
    }

    // Warning: Make sure hasSpace has returned true on parentPtrBlock already before calling this function.
    void addParent(ParentPtrsBlock* parentPtrsBlock, nodeID_t child, nodeID_t parent) {
        auto parentEdgePtr =
            reinterpret_cast<ParentAndNextPtr*>(parentPtrsBlock->block->buffer.data());
        parentEdgePtr[parentPtrsBlock->nextPosToWrite] = ParentAndNextPtr(parent);
        auto curPtr = currFixedParentPtrs.load(std::memory_order_relaxed);
        KU_ASSERT(curPtr != nullptr);
        // Since by default the parentPtr of each node is nullptr, that's what we start with.
        ParentAndNextPtr* expected = nullptr;
        // TODO(Semih): Ask Trevor if this can be compare_exchange_weak?
        while (!curPtr[child.offset].compare_exchange_strong(expected, parentEdgePtr));
        parentEdgePtr->nextPtr.store(expected, std::memory_order_relaxed);
    }

    void fixNodeTable(common::table_id_t tableID) {
        KU_ASSERT(parentPtrs.contains(tableID));
        currFixedParentPtrs.store(
            reinterpret_cast<std::atomic<ParentAndNextPtr*>*>(parentPtrs.at(tableID).get()->buffer.data()), std::memory_order_relaxed);
    }

private:
    std::mutex mtx;
    MemoryManager* mm;
    common::table_id_map_t<std::unique_ptr<MemoryBuffer>> parentPtrs;
    std::atomic<std::atomic<ParentAndNextPtr*>*> currFixedParentPtrs;
    std::vector<std::unique_ptr<ParentPtrsBlock>> blocks;
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
    // that assumes curMultiplicities is already fixed to something.
    void incrementMultiplicity(nodeID_t nodeID, uint64_t multiplicity) {
        fixNodeTable(nodeID.tableID);
        auto curPtr = getCurMultiplicities();
        curPtr[nodeID.offset].fetch_add(multiplicity, std::memory_order_relaxed);
    }

    void incrementMultiplicity(offset_t nodeIDOffset, uint64_t multiplicity) {
        auto curPtr = getCurMultiplicities();
        curPtr[nodeIDOffset].fetch_add(multiplicity);
    }

    uint64_t getMultiplicity(offset_t nodeOffset) {
        auto curPtr = getCurMultiplicities();
        return curPtr[nodeOffset].load(std::memory_order_relaxed);
    }

    void fixNodeTable(common::table_id_t tableID) {
        KU_ASSERT(multiplicitiesMap.contains(tableID));
        curMultiplicities.store(reinterpret_cast<std::atomic<uint64_t>*>(
            multiplicitiesMap.at(tableID).get()->buffer.data()), std::memory_order_relaxed);
    }
private:
    std::atomic<uint64_t>* getCurMultiplicities() {
        auto retVal = curMultiplicities.load(std::memory_order_relaxed);
        KU_ASSERT(retVal != nullptr);
        return retVal;
    }

private:
    common::table_id_map_t<std::unique_ptr<MemoryBuffer>> multiplicitiesMap;
    // curMultiplicities is the multiplicities of the current table that will be updated in a
    // particular rel table extension. This is fixed through the fixNodeTable function.
    std::atomic<std::atomic<uint64_t>*> curMultiplicities;
};

// TODO(Semih): Refactor to two classes, one for DESTINATION_NODES and LENGTHS and the
// other for PATHS. The code will be more explicit that way.
struct AllSPOutputs : public RJOutputs {
    std::unique_ptr<PathLengths> pathLengths;
    // Multiplicities is only used the output type is DESTINATION_NODES or LENGTHS.
    std::unique_ptr<PathMultiplicities> multiplicities;
    std::unique_ptr<ParentPtrsAtomics> parentPtrs;
    explicit AllSPOutputs(graph::Graph* graph, nodeID_t sourceNodeID, RJOutputType outputType,
        MemoryManager* mm = nullptr) : RJOutputs(sourceNodeID, outputType) {
        std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes;
        for (common::table_id_t tableID : graph->getNodeTableIDs()) {
            auto numNodes = graph->getNumNodes(tableID);
            nodeTableIDAndNumNodes.push_back({tableID, numNodes});
        }
        pathLengths = std::make_unique<PathLengths>(nodeTableIDAndNumNodes, mm);
        switch (outputType) {
        case RJOutputType::DESTINATION_NODES:
        case RJOutputType::LENGTHS:
            multiplicities = std::make_unique<PathMultiplicities>(nodeTableIDAndNumNodes, mm);
            break;
        case RJOutputType::PATHS:
            parentPtrs = std::make_unique<ParentPtrsAtomics>(nodeTableIDAndNumNodes, mm);
            break;
        default:
            throw RuntimeException("Unrecognized RJOutputType in "
                                   "AllSPOutputs::AllSPOutputs: " +
                                   std::to_string(static_cast<uint8_t>(outputType)) + ".");
        }
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
        } else {
            parentPtrs->fixNodeTable(nextFrontierTableID);
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
            for (offset_t dstNodeOffset = 0;
                 dstNodeOffset < spOutputs->pathLengths->getNumNodesInCurFrontierFixedNodeTable();
                 ++dstNodeOffset) {
                auto length =
                    spOutputs->pathLengths->getMaskValueFromCurFrontierFixedMask(dstNodeOffset);
                if (length == PathLengths::UNVISITED) {
                    continue;
                }
                if (RJOutputType::LENGTHS == rjOutputType || RJOutputType::PATHS == rjOutputType) {
                    for (uint64_t i = 0; i < spOutputs->multiplicities->getMultiplicity(dstNodeOffset);
                         ++i) {
                        auto dstNodeID = nodeID_t{dstNodeOffset, tableID};
                        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
                        if (RJOutputType::LENGTHS == rjOutputType ||
                            RJOutputType::PATHS == rjOutputType) {
                            lengthVector->setValue<int64_t>(0, length);
                        }
                        fTable.append(vectors);
                    }
                } else if (RJOutputType::PATHS == rjOutputType) {
                    // TODO(Semih): Write the path enumeration logic

                } else {
                    throw RuntimeException("Unrecognized RJOutputType in "
                                           "AllSPOutputWriter::materialize(): " +
                                           std::to_string(static_cast<uint8_t>(rjOutputType)) + ".");
                }
            }
        }
    }

    void writePathsFromDst(AllSPOutputs* allSpOutputs, nodeID_t destinationID) {
        std::stack<ParentAndNextPtr*> stack;
        stack.push(allSpOutputs->parentPtrs->getInitialParentAndNextPtr(destinationID));
        while (!stack.empty()) {
            auto top = stack.top();
            if (top->tableID.load(std::memory_order_relaxed) == allSpOutputs->sourceNodeID.tableID &&
                top->offset.load(std::memory_order_relaxed) == allSpOutputs->sourceNodeID.offset) {
                // We have reached the source node.
                break;
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
                            nbrVal == pathLengthsFrontiers->pathLengths->curIter.load(std::memory_order_relaxed) + 1;
        if (shouldUpdate) {
            // Note: This is safe because curNodeID is in the current frontier, so its
            // shortest paths multiplicity is guaranteed to not change in the current iteration.
            multiplicities->incrementMultiplicity(nbrID.offset,
                multiplicities->getMultiplicity(curNodeID.offset));
        }
        return nbrVal == PathLengths::UNVISITED;
    }

    std::unique_ptr<FrontierCompute> clone() override {
        return std::make_unique<AllSPLengthsFrontierCompute>(this->pathLengthsFrontiers,
            this->multiplicities);
    }

private:
    PathLengthsFrontiers* pathLengthsFrontiers;
    PathMultiplicities* multiplicities;
};

struct AllSPPathsFrontierCompute : public FrontierCompute {
    PathLengthsFrontiers* pathLengthsFrontiers;
    ParentPtrsAtomics* parentPtrs;
    ParentPtrsBlock* parentPtrsBlock;
    explicit AllSPPathsFrontierCompute(PathLengthsFrontiers* pathLengthsFrontiers, ParentPtrsAtomics* parentPtrs)
        : pathLengthsFrontiers{pathLengthsFrontiers}, parentPtrs{parentPtrs} {
                                                          parentPtrsBlock = parentPtrs->addNewBlock();
                                                      };

    bool edgeCompute(nodeID_t curNodeID, nodeID_t nbrID) override {
        auto nbrVal =
            pathLengthsFrontiers->pathLengths->getMaskValueFromNextFrontierFixedMask(nbrID.offset);
        // We should update the nbrID's multiplicity in 2 cases: 1) if nbrID is being visited for
        // the first time, i.e., when its value in the pathLengths frontier is
        // PathLengths::UNVISITED. Or 2) if nbrID has already been visited but in this iteration,
        // so it's value is curIter + 1.
        auto shouldUpdate = nbrVal == PathLengths::UNVISITED ||
                            nbrVal == pathLengthsFrontiers->pathLengths->curIter.load(std::memory_order_relaxed) + 1;
        if (shouldUpdate) {
            if (!parentPtrsBlock->hasSpace()) {
                parentPtrsBlock = parentPtrs->addNewBlock();
            }
            parentPtrs->addParent(parentPtrsBlock, nbrID /* child */, curNodeID /* parent */);
        }
        return nbrVal == PathLengths::UNVISITED;
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
            // TODO(Semih): Fix this as well
//                    case RJOutputType::PATHS: {
//                        auto allPathsFrontiers = std::make_unique<AllSPPathsFrontiers>(
//                            pathLengthsFrontiers->pathLengths.get(), sourceState->singlePaths.get());
//                        auto frontierCompute =
//                            std::make_unique<AllSPPathsFrontierCompute>(singlePathsFrontiers.get());
//                        return std::make_unique<RJCompState>(move(sourceState),
//                        move(singlePathsFrontiers), move(frontierCompute));
//                    }
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
