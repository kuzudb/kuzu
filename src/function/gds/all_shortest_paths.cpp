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
// TODO(Semih): Make all atomic fields of structs/classes private and provide getter functions

struct IterParentAndNextPtr {
    friend class ParentPtrsAtomics;
public:
    IterParentAndNextPtr(uint16_t _iter, nodeID_t parent) {
        iter.store(_iter, std::memory_order_relaxed);
        offset.store(parent.offset, std::memory_order_relaxed);
        tableID.store(parent.tableID, std::memory_order_relaxed);
    }

    IterParentAndNextPtr& operator=(const IterParentAndNextPtr& rhs) {
        iter.store(rhs.iter.load(std::memory_order_relaxed), std::memory_order_relaxed);
        offset.store(rhs.offset.load(std::memory_order_relaxed), std::memory_order_relaxed);
        tableID.store(rhs.tableID.load(std::memory_order_relaxed), std::memory_order_relaxed);
        nextPtr.store(rhs.nextPtr.load(std::memory_order_relaxed), std::memory_order_relaxed);
        return *this;
    }

    void setNextPtr(IterParentAndNextPtr* nextParentPtr) {
        nextPtr.store(nextParentPtr, std::memory_order_relaxed);
    }

    IterParentAndNextPtr* getNextPtr() {
        return nextPtr.load(std::memory_order_relaxed);
    }

    uint16_t getIter() {
        return iter.load(std::memory_order_relaxed);
    }

    nodeID_t getNodeID() {
        return nodeID_t(offset.load(std::memory_order_relaxed),
            tableID.load(std::memory_order_relaxed));
    }

private:
    std::atomic<uint16_t> iter;
    std::atomic<uint64_t> offset;
    std::atomic<uint64_t> tableID;
    std::atomic<IterParentAndNextPtr*> nextPtr;
};

// Each ParentPtrsBlock should be accessed by a single thread.
class ParentPtrsBlock {
    friend class ParentPtrsAtomics;
public:
    ParentPtrsBlock(std::unique_ptr<MemoryBuffer> block, uint64_t sizeInBytes)
        : block{std::move(block)} {
        maxElements.store(sizeInBytes / (sizeof(IterParentAndNextPtr)), std::memory_order_relaxed);
        nextPosToWrite.store(0, std::memory_order_relaxed);
    }

    IterParentAndNextPtr* reserveNextParentAndNextPtr() {
        auto retVal = reinterpret_cast<IterParentAndNextPtr*>(block->buffer.data()) + nextPosToWrite.load(std::memory_order_relaxed);
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
            auto memBuffer = mm->allocateBuffer(false, numNodes * (sizeof(std::atomic<IterParentAndNextPtr*>)));
            auto pointers =
                reinterpret_cast<std::atomic<uint8_t*>*>(memBuffer.get()->buffer.data());
            for (uint64_t i = 0; i < numNodes; ++i) {
                // Note: We are using memory_order_relaxed here because we are assuming that
                // this code is running by a master thread which will run a memory barrier
                // before worker threads start.
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

    IterParentAndNextPtr* getInitialParentAndNextPtr(nodeID_t nodeID) {
        return reinterpret_cast<std::atomic<IterParentAndNextPtr*>*>(
            parentPtrs.at(nodeID.tableID).get()->buffer.data())[nodeID.offset].load(std::memory_order_relaxed);
    }

    // Warning: Make sure hasSpace has returned true on parentPtrBlock already before calling this function.
    void addParent(uint16_t iter, ParentPtrsBlock* parentPtrsBlock, nodeID_t child, nodeID_t parent) {
        auto parentEdgePtr = parentPtrsBlock->reserveNextParentAndNextPtr();
        *parentEdgePtr = IterParentAndNextPtr(iter, parent);
        auto curPtr = currFixedParentPtrs.load(std::memory_order_relaxed);
        KU_ASSERT(curPtr != nullptr);
        // Since by default the parentPtr of each node is nullptr, that's what we start with.
        IterParentAndNextPtr* expected = nullptr;
        while (!curPtr[child.offset].compare_exchange_strong(expected, parentEdgePtr));
        parentEdgePtr->setNextPtr(expected);
    }

    std::atomic<IterParentAndNextPtr*>* getCurFixedParentPtrs() {
        return currFixedParentPtrs.load(std::memory_order_relaxed);
    }

    void fixNodeTable(common::table_id_t tableID) {
        KU_ASSERT(parentPtrs.contains(tableID));
        currFixedParentPtrs.store(
            reinterpret_cast<std::atomic<IterParentAndNextPtr*>*>(parentPtrs.at(tableID).get()->buffer.data()), std::memory_order_relaxed);
    }

private:
    std::mutex mtx;
    MemoryManager* mm;
    common::table_id_map_t<std::unique_ptr<MemoryBuffer>> parentPtrs;
    std::atomic<std::atomic<IterParentAndNextPtr*>*> currFixedParentPtrs;
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
    // it fixes the target node table. This should not be done at a part of the computation when
    // worker threads might be incrementing multiplicity using the incrementTargetMultiplicity
    // function that assumes curTargetMultiplicities is already fixed to something.
    void incrementMultiplicity(nodeID_t nodeID, uint64_t multiplicity) {
        fixTargetNodeTable(nodeID.tableID);
        auto curPtr = getCurTargetMultiplicities();
        curPtr[nodeID.offset].fetch_add(multiplicity);
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

struct AllSPOutputsMultiplicities : public SPOutputs {
    // Multiplicities is only used the output type is DESTINATION_NODES or LENGTHS.
    std::unique_ptr<PathMultiplicities> multiplicities;
    explicit AllSPOutputsMultiplicities(graph::Graph* graph, nodeID_t sourceNodeID,
        MemoryManager* mm = nullptr) : SPOutputs(graph, sourceNodeID, mm) {
        multiplicities = std::make_unique<PathMultiplicities>(nodeTableIDAndNumNodes, mm);
    }

    void initRJFromSource(nodeID_t source) override {
        multiplicities->incrementMultiplicity(source, 1);
    }

    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID, table_id_t nextFrontierTableID) override {
        // Note: We do not fix the node table for pathLengths, which is inherited from AllSPOutputs.
        // See the comment in SingleSPOutputs::beginFrontierComputeBetweenTables() for details.
        multiplicities->fixBoundNodeTable(curFrontierTableID);
        multiplicities->fixTargetNodeTable(nextFrontierTableID);
    };
};

// TODO(Semih): Rename to VarLenAndAllSPOutputPaths
struct AllSPOutputsPaths : public SPOutputs {
    std::unique_ptr<ParentPtrsAtomics> parentPtrs;
    explicit AllSPOutputsPaths(graph::Graph* graph, nodeID_t sourceNodeID,
        MemoryManager* mm = nullptr) : SPOutputs(graph, sourceNodeID, mm) {
        parentPtrs = std::make_unique<ParentPtrsAtomics>(nodeTableIDAndNumNodes, mm);
    }

    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID, table_id_t nextFrontierTableID) override {
        // Note: We do not fix the node table for pathLengths, which is inherited from AllSPOutputs.
        // See the comment in SingleSPOutputs::beginFrontierComputeBetweenTables() for details.
        parentPtrs->fixNodeTable(nextFrontierTableID);
    };
};

class AllSPOutputWriterDsts : public SPOutputWriterDsts {
public:
    explicit AllSPOutputWriterDsts(main::ClientContext* context)
        : SPOutputWriterDsts(context) {}

    std::unique_ptr<RJOutputWriter> clone() override {
        return std::make_unique<AllSPOutputWriterDsts>(context);
    }
protected:
    void fixOtherStructuresToOutputDstsFromNodeTable(RJOutputs* rjOutputs, table_id_t tableID) const override {
        rjOutputs->ptrCast<AllSPOutputsMultiplicities>()->multiplicities->fixTargetNodeTable(tableID);
    }

    void writeMoreAndAppend(processor::FactorizedTable& fTable, RJOutputs* rjOutputs,
        nodeID_t dstNodeID, uint16_t) const override {
        auto multiplicity =
            rjOutputs->ptrCast<AllSPOutputsMultiplicities>()->multiplicities->getTargetMultiplicity(
                dstNodeID.offset);
        for (uint64_t i = 0; i < multiplicity; ++i) {
            fTable.append(vectors);
        }
    }
};

class AllSPOutputWriterLengths : public SPOutputWriterDsts {
public:
    explicit AllSPOutputWriterLengths(main::ClientContext* context)
        : SPOutputWriterDsts(context) {
        lengthVector = std::make_unique<ValueVector>(LogicalType::INT64(), context->getMemoryManager());
        lengthVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(lengthVector.get());
    }

    std::unique_ptr<RJOutputWriter> clone() override {
        return std::make_unique<AllSPOutputWriterLengths>(context);
    }
protected:
    void fixOtherStructuresToOutputDstsFromNodeTable(RJOutputs* rjOutputs, table_id_t tableID) const override {
        rjOutputs->ptrCast<AllSPOutputsMultiplicities>()->multiplicities->fixTargetNodeTable(tableID);
    }

    void writeMoreAndAppend(processor::FactorizedTable& fTable, RJOutputs* rjOutputs,
        nodeID_t dstNodeID, uint16_t length) const override {
        lengthVector->setValue<int64_t>(0, length);
        auto multiplicity =
            rjOutputs->ptrCast<AllSPOutputsMultiplicities>()->multiplicities->getTargetMultiplicity(
                dstNodeID.offset);
        for (uint64_t i = 0; i < multiplicity; ++i) {
            fTable.append(vectors);
        }
    }
private:
    std::unique_ptr<common::ValueVector> lengthVector;
};

class VarLenAndAllSPOutputWriterPaths : public RJOutputWriter {
public:
    explicit VarLenAndAllSPOutputWriterPaths(main::ClientContext* context, uint16_t lowerBound, uint16_t upperBound)
        : RJOutputWriter(context), lowerBound{lowerBound}, upperBound{upperBound} {
        lengthVector = std::make_unique<ValueVector>(LogicalType::INT64(), context->getMemoryManager());
        lengthVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(lengthVector.get());
        pathNodeIDsVector =
            std::make_unique<ValueVector>(LogicalType::LIST(LogicalType::INTERNAL_ID()), context->getMemoryManager());
        pathNodeIDsVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(pathNodeIDsVector.get());
    }

    void beginWritingForDstNodesInTable(RJOutputs* rjOutputs, table_id_t tableID) const override {
        rjOutputs->ptrCast<AllSPOutputsPaths>()->parentPtrs->fixNodeTable(tableID);
    }

    void write(RJOutputs* rjOutputs, processor::FactorizedTable& fTable,
        nodeID_t dstNodeID) const override {
        AllSPOutputsPaths* allSpOutputs = rjOutputs->ptrCast<AllSPOutputsPaths>();
        PathVectorWriter writer(pathNodeIDsVector.get());
        auto firstParent = allSpOutputs->parentPtrs->getCurFixedParentPtrs()[dstNodeID.offset].load(std::memory_order_relaxed);
        if (firstParent == nullptr) {
            // This case should only run for variable length joins.
            dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
            lengthVector->setValue<int64_t>(0, 0);
            writer.beginWritingNewPath(0);
            fTable.append(vectors);
            return;
        }
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        std::cout << "outside while loop setting length to: " << firstParent->getIter() << std::endl;
        lengthVector->setValue<int64_t>(0, firstParent->getIter());
        std::vector<IterParentAndNextPtr*> curPath;
        // TODO(Semih): Remove
//        auto firstParent = allSpOutputs->parentPtrs->getInitialParentAndNextPtr(dstNodeID);
        // If the firstParent == nullptr then this dstNodeID must be srcNodeID

        curPath.push_back(firstParent);
        auto backtracking = false;
        while (!curPath.empty()) {
            auto top = curPath[curPath.size()-1];
            auto topNodeID = top->getNodeID();
            if (topNodeID == allSpOutputs->sourceNodeID) {
                auto curPathSize = curPath.size();
                writer.beginWritingNewPath(curPathSize);
                while (writer.nextPathPos > 0) {
                    writer.addNewNodeID(curPath[curPathSize - (writer.nextPathPos + 1)]->getNodeID());
                }
                fTable.append(vectors);
                backtracking = true;
            }
            if (backtracking) {
                // This code checks if we should switch from backtracking to forward-tracking, i.e.,
                // moving forward in the DFS logic to find paths. We switch from backtracking if:
                // (i) the current top element in the stack has a nextPtr, i.e., the top node has
                // more parent edges in the BFS graph AND:
                // (ii.1) if this is the first element in the stack (curPath.size() == 1), i.e., we
                // are enumerating the parents of the destination, then we should switch to
                // forward-tracking if the next parent has visited the destination at a length
                // that's greater than or equal to the lower bound of the recursive join. Otherwise,
                // we'll enumerate paths that are smaller than the lower; OR
                // (ii.2) if this is not the first element in the stack, i.e., then we should switch
                // to forward tracking only if the next parent of the top node in the stack has the
                // same iter value as the current parent. That's because the levels/iter need to
                // decrease by 1 each time we add a new node in the stack.
                if (top->getNextPtr() != nullptr &&
                    ((curPath.size() == 1 && top->getNextPtr()->getIter() >= lowerBound) || top->getNextPtr()->getIter() == top->getIter())) {
                    curPath[curPath.size() - 1] = top->getNextPtr();
                    backtracking = false;
                    if (curPath.size() == 1) {
                        lengthVector->setValue<int64_t>(0, curPath[0]->getIter());
                    }
                } else {
                    curPath.pop_back();
                }
            } else {
                auto parent = allSpOutputs->parentPtrs->getInitialParentAndNextPtr(topNodeID);
                while (parent->getIter() != top->getIter() - 1) {
                    parent = parent->getNextPtr();
                }
                curPath.push_back(parent);
                backtracking = false;
            }
        }
    }

protected:
    uint16_t lowerBound;
    uint16_t upperBound;
    std::unique_ptr<common::ValueVector> lengthVector;
    std::unique_ptr<common::ValueVector> pathNodeIDsVector;
};


class AllSPOutputWriterPaths : public VarLenAndAllSPOutputWriterPaths {
public:
    explicit AllSPOutputWriterPaths(main::ClientContext* context, uint16_t upperBound)
        : VarLenAndAllSPOutputWriterPaths(context, 1 /* lower bound */, upperBound) {}

    bool skipWriting(RJOutputs* rjOutputs, nodeID_t dstNodeID) const override {
        auto allSpOutputsPaths = rjOutputs->ptrCast<AllSPOutputsPaths>();
        // For all shortest path computations, we do not output any results from source to source.
        // We also do not output any results if a destination node has not been reached.
        return dstNodeID == allSpOutputsPaths->sourceNodeID || nullptr == allSpOutputsPaths->parentPtrs->getCurFixedParentPtrs()[dstNodeID.offset].load(
                   std::memory_order_relaxed);
    }

    std::unique_ptr<RJOutputWriter> clone() override {
        return std::make_unique<AllSPOutputWriterPaths>(context, upperBound);
    }
};

class VarLenOutputWriterPaths : public VarLenAndAllSPOutputWriterPaths {
public:
    explicit VarLenOutputWriterPaths(
        main::ClientContext* context, uint16_t lowerBound, uint16_t upperBound)
        : VarLenAndAllSPOutputWriterPaths(context, lowerBound, upperBound) {}

    bool skipWriting(RJOutputs* rjOutputs, nodeID_t dstNodeID) const override {
        // TODO(Semih): Rename the variable to varlenOutputPaths or outputPaths;
        auto allSpOutputsPaths = rjOutputs->ptrCast<AllSPOutputsPaths>();
        auto firstIterParentAndNextPtr = allSpOutputsPaths->parentPtrs->getCurFixedParentPtrs()[dstNodeID.offset].load(
            std::memory_order_relaxed);
        // For variable lengths joins, we skip a destination node d in the following conditions:
        //    (i) if no path has reached d from the source, except when the lower bound is 0.
        //    (ii) the longest path that has reached d, which is stored in the iter value of the
        //    firstIterParentAndNextPtr is smaller than the lower bound.
        // We also do not output any results if a destination node has not been reached.
        // TODO(Semih): Remove
        if (nullptr == firstIterParentAndNextPtr) {
            return lowerBound > 0;
        } else {
            std::cout << "firstIterParentAndNextPtr->getIter(): "
                      << firstIterParentAndNextPtr->getIter() << " lowerBound: " << lowerBound
                      << std::endl;
            return firstIterParentAndNextPtr->getIter() < lowerBound;
        }
    }

    std::unique_ptr<RJOutputWriter> clone() override {
        return std::make_unique<VarLenOutputWriterPaths>(context, lowerBound, upperBound);
    }
};

struct AllSPLengthsEdgeCompute : public EdgeCompute {
    explicit AllSPLengthsEdgeCompute(PathLengthsFrontiers* pathLengthsFrontiers,
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
                            nbrVal == pathLengthsFrontiers->pathLengths->curIter.load(std::memory_order_relaxed);
        if (shouldUpdate) {
            // Note: This is safe because curNodeID is in the current frontier, so its
            // shortest paths multiplicity is guaranteed to not change in the current iteration.
            multiplicities->incrementTargetMultiplicity(nbrID.offset,
                multiplicities->getBoundMultiplicity(curNodeID.offset));
        }
        return nbrVal == PathLengths::UNVISITED;
    }

    std::unique_ptr<EdgeCompute> clone() override {
        return std::make_unique<AllSPLengthsEdgeCompute>(this->pathLengthsFrontiers,
            this->multiplicities);
    }

private:
    PathLengthsFrontiers* pathLengthsFrontiers;
    PathMultiplicities* multiplicities;
};

struct AllSPPathsEdgeCompute : public EdgeCompute {
    PathLengthsFrontiers* pathLengthsFrontiers;
    ParentPtrsAtomics* parentPtrs;
    ParentPtrsBlock* parentPtrsBlock;
    explicit AllSPPathsEdgeCompute(PathLengthsFrontiers* pathLengthsFrontiers, ParentPtrsAtomics* parentPtrs)
        : pathLengthsFrontiers{pathLengthsFrontiers}, parentPtrs{parentPtrs} {
                                                          parentPtrsBlock = parentPtrs->addNewBlock();
                                                      };

    bool edgeCompute(nodeID_t curNodeID, nodeID_t nbrID) override {
        auto nbrLen =
            pathLengthsFrontiers->pathLengths->getMaskValueFromNextFrontierFixedMask(nbrID.offset);
        // We should update the nbrID's multiplicity in 2 cases: 1) if nbrID is being visited for
        // the first time, i.e., when its value in the pathLengths frontier is
        // PathLengths::UNVISITED. Or 2) if nbrID has already been visited but in this iteration,
        // so it's value is curIter + 1.
        auto shouldUpdate =
            nbrLen == PathLengths::UNVISITED ||
            nbrLen == pathLengthsFrontiers->pathLengths->curIter.load(std::memory_order_relaxed);
        if (shouldUpdate) {
            // TODO(Semih): Add a test case that triggers this.
            if (!parentPtrsBlock->hasSpace()) {
                parentPtrsBlock = parentPtrs->addNewBlock();
            }
            parentPtrs->addParent(pathLengthsFrontiers->curIter.load(std::memory_order_relaxed), parentPtrsBlock, nbrID /* child */, curNodeID /* parent */);
        }
        return nbrLen == PathLengths::UNVISITED;
    }

    std::unique_ptr<EdgeCompute> clone() override {
        return std::make_unique<AllSPPathsEdgeCompute>(this->pathLengthsFrontiers,
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
        switch (outputType) {
        case RJOutputType::DESTINATION_NODES:
            outputWriter = std::make_unique<AllSPOutputWriterDsts>(context);
            break;
        case RJOutputType::LENGTHS:
            outputWriter = std::make_unique<AllSPOutputWriterLengths>(context);
            break;
        case RJOutputType::PATHS:
            outputWriter = std::make_unique<AllSPOutputWriterPaths>(context,
                bindData->ptrCast<RJBindData>()->upperBound);
            break;
        default:
            throw RuntimeException("Unrecognized RJOutputType in "
                                   "AllSPAlgorithm::initLocalState(): " +
                                   std::to_string(static_cast<uint8_t>(outputType)) + ".");
        }
    }

    std::unique_ptr<RJCompState> getFrontiersAndEdgeCompute(
        processor::ExecutionContext* executionContext, nodeID_t sourceNodeID) override {
        std::unique_ptr<SPOutputs> spOutputs;
        if (outputType == RJOutputType::PATHS) {
            spOutputs = std::make_unique<AllSPOutputsPaths>(sharedState->graph.get(), sourceNodeID,
                executionContext->clientContext->getMemoryManager());
        } else {
            spOutputs = std::make_unique<AllSPOutputsMultiplicities>(sharedState->graph.get(),
                sourceNodeID, executionContext->clientContext->getMemoryManager());
        }
        auto pathLengthsFrontiers =
            std::make_unique<PathLengthsFrontiers>(spOutputs->pathLengths,
                executionContext->clientContext->getMaxNumThreadForExec());
        switch (outputType) {
        case RJOutputType::DESTINATION_NODES:
        case RJOutputType::LENGTHS: {
            auto edgeCompute = std::make_unique<AllSPLengthsEdgeCompute>(
                pathLengthsFrontiers.get(), spOutputs->ptrCast<AllSPOutputsMultiplicities>()->multiplicities.get());
            return std::make_unique<RJCompState>(move(spOutputs), move(pathLengthsFrontiers),
                move(edgeCompute));
        }
        case RJOutputType::PATHS: {
            auto edgeCompute = std::make_unique<AllSPPathsEdgeCompute>(
                pathLengthsFrontiers.get(), spOutputs->ptrCast<AllSPOutputsPaths>()->parentPtrs.get());
            return std::make_unique<RJCompState>(
                move(spOutputs), move(pathLengthsFrontiers), move(edgeCompute));
        }
        default:
            throw RuntimeException("Unrecognized RJOutputType in "
                                   "RJAlgorithm::getFrontiersAndEdgeCompute(): " +
                                   std::to_string(static_cast<uint8_t>(outputType)) + ".");
        }
    }
};

struct VarLenJoinsEdgeCompute : public EdgeCompute {
    DoublePathLengthsFrontiers* doublePathLengthsFrontiers;
    ParentPtrsAtomics* parentPtrs;
    ParentPtrsBlock* parentPtrsBlock;
    explicit VarLenJoinsEdgeCompute(DoublePathLengthsFrontiers* doublePathLengthsFrontiers, ParentPtrsAtomics* parentPtrs)
        : doublePathLengthsFrontiers{doublePathLengthsFrontiers}, parentPtrs{parentPtrs} {
        parentPtrsBlock = parentPtrs->addNewBlock();
    };

    bool edgeCompute(nodeID_t curNodeID, nodeID_t nbrID) override {
        // We should always update the nbrID in variable length joins
        if (!parentPtrsBlock->hasSpace()) {
            parentPtrsBlock = parentPtrs->addNewBlock();
        }
        parentPtrs->addParent(doublePathLengthsFrontiers->curIter.load(std::memory_order_relaxed),
            parentPtrsBlock, nbrID /* child */, curNodeID /* parent */);
        return true;
    }

    std::unique_ptr<EdgeCompute> clone() override {
        return std::make_unique<VarLenJoinsEdgeCompute>(this->doublePathLengthsFrontiers,
            this->parentPtrs);
    }
};

/**
 * Algorithm for parallel all shortest paths computation, so all shortest paths from a source to
 * is returned for each destination. If paths are not returned, multiplicities indicate the number
 * of paths to each destination.
 */
class VarLenJoinsAlgorithm final : public RJAlgorithm {

public:
    explicit VarLenJoinsAlgorithm() : RJAlgorithm(RJOutputType::PATHS, true /* hasLowerBoundInput */){}
    VarLenJoinsAlgorithm(const VarLenJoinsAlgorithm& other) : RJAlgorithm(other) {}

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<VarLenJoinsAlgorithm>(*this);
    }

protected:
    void initLocalState(main::ClientContext* context) override {
        outputWriter = std::make_unique<VarLenOutputWriterPaths>(context,
            bindData->ptrCast<RJBindData>()->lowerBound,
            bindData->ptrCast<RJBindData>()->upperBound);
    }

    std::unique_ptr<RJCompState> getFrontiersAndEdgeCompute(
        processor::ExecutionContext* executionContext, nodeID_t sourceNodeID) override {
        std::unique_ptr<RJOutputs> spOutputs =
            std::make_unique<AllSPOutputsPaths>(sharedState->graph.get(), sourceNodeID,
                executionContext->clientContext->getMemoryManager());
        // TODO(Semih): We need to pass a pathLengthFrontier that uses 2 copies of masks internally.
        // TODO(Semih): We also need a way to take in the minimum and the maximum path lengths
        auto doublePathLengthsFrontiers =
            std::make_unique<DoublePathLengthsFrontiers>(spOutputs->nodeTableIDAndNumNodes,
                executionContext->clientContext->getMaxNumThreadForExec(), executionContext->clientContext->getMemoryManager());
        auto edgeCompute = std::make_unique<VarLenJoinsEdgeCompute>(
            doublePathLengthsFrontiers.get(), spOutputs->ptrCast<AllSPOutputsPaths>()->parentPtrs.get());
        return std::make_unique<RJCompState>(
            move(spOutputs), move(doublePathLengthsFrontiers), move(edgeCompute));
    }
};

function_set VarLenJoinsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name,
        std::make_unique<VarLenJoinsAlgorithm>());
    result.push_back(std::move(function));
    return result;
}

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
