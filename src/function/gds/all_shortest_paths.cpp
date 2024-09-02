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
    friend class ParentPtrsAtomics;
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

    ParentAndNextPtr* getNextPtr() {
        return nextPtr.load(std::memory_order_relaxed);
    }

    nodeID_t getNodeID() {
        return nodeID_t(offset.load(std::memory_order_relaxed),
            tableID.load(std::memory_order_relaxed));
    }

    // TODO(Semih): make these private and provide getter and setter functions that do the atomic
    // operations. That way all store load operations would be localized here.
    // TODO(Semih): Similarly do the same for other structs and fields.
private:
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
//        std::cout << "sizeInBytes: " << sizeInBytes
//                  << "sizeof(ParentAndNextPtr):" << sizeof(ParentAndNextPtr)
//                  << " maxElements: " << maxElements.load(std::memory_order_relaxed) << std::endl;
    }

    ParentAndNextPtr* reserveNextParentAndNextPtr() {
        // TODO(Semih): Double check that this pointer arithmetic is correct
        auto retVal = reinterpret_cast<ParentAndNextPtr*>(block->buffer.data()) + nextPosToWrite.load(std::memory_order_relaxed);
        nextPosToWrite.fetch_add(1, std::memory_order_relaxed);
        return retVal;
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
        // TODO(Semih): Remove
//        std::cout << "addParent called. child: " << child.offset << " parent: " << parent.offset << std::endl;
        auto parentEdgePtr = parentPtrsBlock->reserveNextParentAndNextPtr();
        *parentEdgePtr = ParentAndNextPtr(parent);
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
    // TODO(Semih): Add getter functions to ensure we read this atomic with the right memory order.
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
    // worker threads might be incrementing multiplicity using the incrementTargetMultiplicity function
    // that assumes curTargetMultiplicities is already fixed to something.
    void incrementMultiplicity(nodeID_t nodeID, uint64_t multiplicity) {
        fixTargetNodeTable(nodeID.tableID);
        auto curPtr = getCurTargetMultiplicities();
        curPtr[nodeID.offset].fetch_add(multiplicity, std::memory_order_relaxed);
    }

    void incrementTargetMultiplicity(offset_t nodeIDOffset, uint64_t multiplicity) {
        getCurTargetMultiplicities()[nodeIDOffset].fetch_add(multiplicity);
    }

    uint64_t getBoundMultiplicity(offset_t nodeOffset) {
        return getCurBoundMultiplicities()[nodeOffset].load(std::memory_order_relaxed);
    }

    uint64_t getTargetMultiplicity(offset_t nodeOffset) {
        return getCurTargetMultiplicities()[nodeOffset].load(std::memory_order_relaxed);
    }

    void fixBoundNodeTable(common::table_id_t tableID) {
        KU_ASSERT(multiplicitiesMap.contains(tableID));
        curBoundMultiplicities.store(reinterpret_cast<std::atomic<uint64_t>*>(
                                          multiplicitiesMap.at(tableID).get()->buffer.data()), std::memory_order_relaxed);
    }

    void fixTargetNodeTable(common::table_id_t tableID) {
        KU_ASSERT(multiplicitiesMap.contains(tableID));
        curTargetMultiplicities.store(reinterpret_cast<std::atomic<uint64_t>*>(
            multiplicitiesMap.at(tableID).get()->buffer.data()), std::memory_order_relaxed);
    }
private:
    std::atomic<uint64_t>* getCurTargetMultiplicities() {
        auto retVal = curTargetMultiplicities.load(std::memory_order_relaxed);
        KU_ASSERT(retVal != nullptr);
        return retVal;
    }
    std::atomic<uint64_t>* getCurBoundMultiplicities() {
        auto retVal = curBoundMultiplicities.load(std::memory_order_relaxed);
        KU_ASSERT(retVal != nullptr);
        return retVal;
    }

private:
    common::table_id_map_t<std::unique_ptr<MemoryBuffer>> multiplicitiesMap;
    // curTargetMultiplicities is the multiplicities of the current table that will be updated in a
    // particular rel table extension. This is fixed through the fixTargetNodeTable function.
    std::atomic<std::atomic<uint64_t>*> curTargetMultiplicities;
    // curBoundMultiplicities is the multiplicities of the table "from which" an extension is
    // being made. This is fixed through the fixBoundNodeTable function.
    std::atomic<std::atomic<uint64_t>*> curBoundMultiplicities;
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
        if (RJOutputType::DESTINATION_NODES == outputType || RJOutputType::LENGTHS == outputType) {
            multiplicities->incrementMultiplicity(source, 1);
        }
    }

    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID, table_id_t nextFrontierTableID) override {
        // Note: We do not fix the node table for pathLengths. See the comment in
        // SingleSPOutputs::beginFrontierComputeBetweenTables() for details.
        // Note: We do not need to keep track of multiplicities when we're keeping track of paths,
        // since the number of paths to each destination d will equal the multiplicity of d.
        if (RJOutputType::DESTINATION_NODES == outputType || RJOutputType::LENGTHS == outputType) {
            multiplicities->fixBoundNodeTable(curFrontierTableID);
            multiplicities->fixTargetNodeTable(nextFrontierTableID);
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
            if (RJOutputType::DESTINATION_NODES == rjOutputType || RJOutputType::LENGTHS == rjOutputType) {
                spOutputs->multiplicities->fixTargetNodeTable(tableID);
            }
            spOutputs->pathLengths->fixCurFrontierNodeTable(tableID);
            for (offset_t dstNodeOffset = 0; // dstNodeOffset < 5;
                 dstNodeOffset < spOutputs->pathLengths->getNumNodesInCurFrontierFixedNodeTable();
                 ++dstNodeOffset) {
                auto length =
                    spOutputs->pathLengths->getMaskValueFromCurFrontierFixedMask(dstNodeOffset);
                if (length == PathLengths::UNVISITED) {
                    continue;
                }
                auto multiplicity = RJOutputType::PATHS == rjOutputType ?
                                        1 :
                        spOutputs->multiplicities->getTargetMultiplicity(dstNodeOffset);
                for (uint64_t i = 0; i < multiplicity; ++i) {
                    auto dstNodeID = nodeID_t{dstNodeOffset, tableID};
                    dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
                    if (RJOutputType::LENGTHS == rjOutputType ||
                        RJOutputType::PATHS == rjOutputType) {
                        lengthVector->setValue<int64_t>(0, length);
                        if (RJOutputType::PATHS == rjOutputType) {
                            writePathsFromDst(fTable, spOutputs, dstNodeID);
                        }
                    }
                    if (RJOutputType::DESTINATION_NODES == rjOutputType ||
                        RJOutputType::LENGTHS == rjOutputType) {
                        fTable.append(vectors);
                    }
                }
            }
        }
    }

    void writePathsFromDst(processor::FactorizedTable& fTable, AllSPOutputs* allSpOutputs, nodeID_t dstNodeID) const {
        std::vector<ParentAndNextPtr*> curPath;
        PathVectorWriter writer(pathNodeIDsVector.get());
        auto firstParent = allSpOutputs->parentPtrs->getInitialParentAndNextPtr(dstNodeID);
        // If the firstParent == nullptr then this dstNodeID must be srcNodeID
        if (firstParent == nullptr) {
            KU_ASSERT(allSpOutputs->sourceNodeID == dstNodeID);
            writer.beginWritingNewPath(0);
            fTable.append(vectors);
            return;
        }
        curPath.push_back(firstParent);
        auto backtracking = false;
        while (!curPath.empty()) {
            auto top = curPath[curPath.size()-1];
            auto topNodeID = top->getNodeID();
            // TODO(Semih): Remove
//            std::cout << "topNodeID: " << topNodeID.offset << std::endl;
            if (topNodeID == allSpOutputs->sourceNodeID) {
                // TODO(Semih): There is the possibility of multiple direct edges from the source.
                // Test that case.
                auto len = curPath.size();
                writer.beginWritingNewPath(len);
                while (writer.curIntNbrIndex > 0) {
                    writer.addNewNodeID(curPath[len - (writer.curIntNbrIndex + 1)]->getNodeID());
                }
                fTable.append(vectors);
                backtracking = true;
            }
            if (backtracking) {
                // TODO(Semih): Remove
//                std::cout << "backtracking. nextPtr: " << (top->getNextPtr() == nullptr ? " nullPtr" : " NOT nullPtr") << std::endl;
//                if (top->getNextPtr() != nullptr) {
//                    std::cout << "backtracking. nextPtr.nodeID: " << top->getNextPtr()->getNodeID().offset << std::endl;
//                }
                if (top->getNextPtr() != nullptr) {
                    curPath[curPath.size() - 1] = top->getNextPtr();
                    backtracking = false;
                } else {
                    curPath.pop_back();
                }
            } else {
                curPath.push_back(allSpOutputs->parentPtrs->getInitialParentAndNextPtr(topNodeID));
                backtracking = false;
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
            multiplicities->incrementTargetMultiplicity(nbrID.offset,
                multiplicities->getBoundMultiplicity(curNodeID.offset));
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
//        // TODO(Semih): Remove
//        std::cout << "edgeCompute called. curNodeID: " << curNodeID.offset
//                  << " nbrID: " << nbrID.offset
//                  << " shouldUpdate: " << (shouldUpdate ? " true" : " false") << std::endl;
        if (shouldUpdate) {
            // TODO(Semih): Add a test case that triggers this.
            if (!parentPtrsBlock->hasSpace()) {
                parentPtrsBlock = parentPtrs->addNewBlock();
            }
            parentPtrs->addParent(parentPtrsBlock, nbrID /* child */, curNodeID /* parent */);
        }
        return nbrVal == PathLengths::UNVISITED;
    }

    std::unique_ptr<FrontierCompute> clone() override {
        return std::make_unique<AllSPPathsFrontierCompute>(this->pathLengthsFrontiers,
            this->parentPtrs);
    }
};

/**
 * Algorithm for parallel all shortest paths computation, so all shortest paths from a source to
 * is returned for each destination. If paths are not returned, multiplicities indicate the number
 * of paths to each destination.
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
        auto pathLengthsFrontiers =
            std::make_unique<PathLengthsFrontiers>(spOutputs->pathLengths.get(),
                executionContext->clientContext->getMaxNumThreadForExec());
        switch (outputType) {
        case RJOutputType::DESTINATION_NODES:
        case RJOutputType::LENGTHS: {
            auto frontierCompute = std::make_unique<AllSPLengthsFrontierCompute>(
                pathLengthsFrontiers.get(), spOutputs->multiplicities.get());
            return std::make_unique<RJCompState>(move(spOutputs), move(pathLengthsFrontiers),
                move(frontierCompute));
        }
        case RJOutputType::PATHS: {
            auto frontierCompute = std::make_unique<AllSPPathsFrontierCompute>(
                pathLengthsFrontiers.get(), spOutputs->parentPtrs.get());
            return std::make_unique<RJCompState>(
                move(spOutputs), move(pathLengthsFrontiers), move(frontierCompute));
        }
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
