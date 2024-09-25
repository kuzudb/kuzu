#include "binder/expression/expression_util.h"
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

struct ParentIterAndNextPtr {
    friend class ParentPtrsAtomics;

public:
    ParentIterAndNextPtr(uint16_t iter_, common::nodeID_t parent) {
        setIterAndNodeID(iter_, parent);
    }

    void setIterAndNodeID(uint16_t iter_, common::nodeID_t parent) {
        iter.store(iter_, std::memory_order_relaxed);
        offset.store(parent.offset, std::memory_order_relaxed);
        tableID.store(parent.tableID, std::memory_order_relaxed);
    }

    void setNextPtr(ParentIterAndNextPtr* nextParentPtr) {
        nextPtr.store(nextParentPtr, std::memory_order_relaxed);
    }

    ParentIterAndNextPtr* getNextPtr() { return nextPtr.load(std::memory_order_relaxed); }

    uint16_t getIter() { return iter.load(std::memory_order_relaxed); }

    common::nodeID_t getNodeID() {
        return common::nodeID_t(offset.load(std::memory_order_relaxed),
            tableID.load(std::memory_order_relaxed));
    }

private:
    std::atomic<uint16_t> iter;
    std::atomic<uint64_t> offset;
    std::atomic<uint64_t> tableID;
    std::atomic<ParentIterAndNextPtr*> nextPtr;
};

// Each ParentPtrsBlock should be accessed by a single thread.
class ParentPtrsBlock {
    friend class ParentPtrsAtomics;

public:
    ParentPtrsBlock(std::unique_ptr<MemoryBuffer> block, uint64_t sizeInBytes)
        : block{std::move(block)} {
        maxElements.store(sizeInBytes / (sizeof(ParentIterAndNextPtr)), std::memory_order_relaxed);
        nextPosToWrite.store(0, std::memory_order_relaxed);
    }

    ParentIterAndNextPtr* reserveNextParentAndNextPtr() {
        auto retVal = reinterpret_cast<ParentIterAndNextPtr*>(block->buffer.data()) +
                      nextPosToWrite.load(std::memory_order_relaxed);
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
    static constexpr uint64_t ALL_PATHS_BLOCK_SIZE = (std::uint64_t)1 << 19;

public:
    explicit ParentPtrsAtomics(
        std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes, MemoryManager* mm)
        : mm{mm} {
        for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
            auto memBuffer =
                mm->allocateBuffer(false, numNodes * (sizeof(std::atomic<ParentIterAndNextPtr*>)));
            auto pointers =
                reinterpret_cast<std::atomic<uint8_t*>*>(memBuffer.get()->buffer.data());
            for (uint64_t i = 0; i < numNodes; ++i) {
                // Note: We are using memory_order_relaxed here because we are assuming that
                // this code is running by a master thread which will run a memory barrier
                // before worker threads start.
                pointers[i].store(nullptr, std::memory_order_relaxed);
            }
            parentPtrs.insert({tableID, std::move(memBuffer)});
        }
    }

    // This function is thread safe and should be called by a worker thread Ti to grab a block
    // of memory that Ti owns and writes to.
    ParentPtrsBlock* addNewBlock() {
        std::unique_lock lck{mtx};
        auto memBlock = mm->allocateBuffer(false /* don't init to 0 */, ALL_PATHS_BLOCK_SIZE);
        blocks.push_back(
            std::make_unique<ParentPtrsBlock>(std::move(memBlock), ALL_PATHS_BLOCK_SIZE));
        return blocks[blocks.size() - 1].get();
    }

    ParentIterAndNextPtr* getInitialParentAndNextPtr(common::nodeID_t nodeID) {
        return reinterpret_cast<std::atomic<ParentIterAndNextPtr*>*>(
            parentPtrs.at(nodeID.tableID).get()->buffer.data())[nodeID.offset]
            .load(std::memory_order_relaxed);
    }

    // Warning: Make sure hasSpace has returned true on parentPtrBlock already before calling this
    // function.
    void addParent(uint16_t iter, ParentPtrsBlock* parentPtrsBlock, common::nodeID_t child,
        common::nodeID_t parent) {
        auto parentEdgePtr = parentPtrsBlock->reserveNextParentAndNextPtr();
        parentEdgePtr->setIterAndNodeID(iter, parent);
        //        *parentEdgePtr = ParentIterAndNextPtr(iter, parent);
        auto curPtr = currFixedParentPtrs.load(std::memory_order_relaxed);
        KU_ASSERT(curPtr != nullptr);
        // Since by default the parentPtr of each node is nullptr, that's what we start with.
        ParentIterAndNextPtr* expected = nullptr;
        while (!curPtr[child.offset].compare_exchange_strong(expected, parentEdgePtr))
            ;
        parentEdgePtr->setNextPtr(expected);
    }

    std::atomic<ParentIterAndNextPtr*>* getCurFixedParentPtrs() {
        return currFixedParentPtrs.load(std::memory_order_relaxed);
    }

    void fixNodeTable(common::table_id_t tableID) {
        KU_ASSERT(parentPtrs.contains(tableID));
        currFixedParentPtrs.store(reinterpret_cast<std::atomic<ParentIterAndNextPtr*>*>(
                                      parentPtrs.at(tableID).get()->buffer.data()),
            std::memory_order_relaxed);
    }

private:
    std::mutex mtx;
    MemoryManager* mm;
    common::table_id_map_t<std::unique_ptr<MemoryBuffer>> parentPtrs;
    std::atomic<std::atomic<ParentIterAndNextPtr*>*> currFixedParentPtrs;
    std::vector<std::unique_ptr<ParentPtrsBlock>> blocks;
};

/**
 * A dense storage structures for multiplicities for multiple node tables.
 */
class PathMultiplicities {
public:
    explicit PathMultiplicities(
        std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
        MemoryManager* mm) {
        for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
            auto memoryBuffer = mm->allocateBuffer(false, numNodes * sizeof(std::atomic<uint64_t>));
            // Question to Trevor: Do I need to use atomics? If so, why?
            auto multiplicitiesPtr =
                reinterpret_cast<std::atomic<uint64_t>*>(memoryBuffer->buffer.data());
            for (uint64_t i = 0; i < numNodes; ++i) {
                multiplicitiesPtr[i].store(0);
            }
            multiplicitiesMap.insert({tableID, std::move(memoryBuffer)});
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
                                         multiplicitiesMap.at(tableID).get()->buffer.data()),
            std::memory_order_relaxed);
    }

    void fixTargetNodeTable(common::table_id_t tableID) {
        KU_ASSERT(multiplicitiesMap.contains(tableID));
        curTargetMultiplicities.store(reinterpret_cast<std::atomic<uint64_t>*>(
                                          multiplicitiesMap.at(tableID).get()->buffer.data()),
            std::memory_order_relaxed);
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

struct AllSPMultiplicitiesOutputs : public SPOutputs {

public:
    explicit AllSPMultiplicitiesOutputs(
        std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
        common::nodeID_t sourceNodeID, MemoryManager* mm = nullptr)
        : SPOutputs(nodeTableIDAndNumNodes, sourceNodeID, mm) {
        multiplicities = std::make_unique<PathMultiplicities>(nodeTableIDAndNumNodes, mm);
    }

    void initRJFromSource(common::nodeID_t source) override {
        multiplicities->incrementMultiplicity(source, 1);
    }

    void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) override {
        // Note: We do not fix the node table for pathLengths, which is inherited from AllSPOutputs.
        // See the comment in SingleSPOutputs::beginFrontierComputeBetweenTables() for details.
        multiplicities->fixBoundNodeTable(curFrontierTableID);
        multiplicities->fixTargetNodeTable(nextFrontierTableID);
    };

    void beginWritingOutputsForDstNodesInTable(table_id_t tableID) override {
        pathLengths->fixCurFrontierNodeTable(tableID);
        multiplicities->fixTargetNodeTable(tableID);
    }

public:
    std::unique_ptr<PathMultiplicities> multiplicities;
};

struct VarLenOrAllSPPathsOutputs : public SPOutputs {
    explicit VarLenOrAllSPPathsOutputs(
        std::unordered_map<common::table_id_t, uint64_t> nodeTableIDAndNumNodes,
        common::nodeID_t sourceNodeID, MemoryManager* mm = nullptr)
        : SPOutputs(nodeTableIDAndNumNodes, sourceNodeID, mm) {
        parentPtrs = std::make_unique<ParentPtrsAtomics>(nodeTableIDAndNumNodes, mm);
    }

    void beginFrontierComputeBetweenTables(table_id_t, table_id_t nextFrontierTableID) override {
        // Note: We do not fix the node table for pathLengths, which is inherited from AllSPOutputs.
        // See the comment in SingleSPOutputs::beginFrontierComputeBetweenTables() for details.
        parentPtrs->fixNodeTable(nextFrontierTableID);
    };

    void beginWritingOutputsForDstNodesInTable(table_id_t tableID) override {
        pathLengths->fixCurFrontierNodeTable(tableID);
        parentPtrs->fixNodeTable(tableID);
    }

public:
    std::unique_ptr<ParentPtrsAtomics> parentPtrs;
};

class AllSPOutputWriterDsts : public SPOutputWriterDsts {
public:
    explicit AllSPOutputWriterDsts(main::ClientContext* context, RJOutputs* rjOutputs)
        : SPOutputWriterDsts(context, rjOutputs) {}

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<AllSPOutputWriterDsts>(context, rjOutputs);
    }

protected:
    void writeMoreAndAppend(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        uint16_t) const override {
        auto multiplicity =
            rjOutputs->ptrCast<AllSPMultiplicitiesOutputs>()->multiplicities->getTargetMultiplicity(
                dstNodeID.offset);
        for (uint64_t i = 0; i < multiplicity; ++i) {
            fTable.append(vectors);
        }
    }
};

class AllSPOutputWriterLengths : public SPOutputWriterDsts {
public:
    explicit AllSPOutputWriterLengths(main::ClientContext* context, RJOutputs* rjOutputs)
        : SPOutputWriterDsts(context, rjOutputs) {
        lengthVector =
            std::make_unique<ValueVector>(LogicalType::INT64(), context->getMemoryManager());
        lengthVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(lengthVector.get());
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<AllSPOutputWriterLengths>(context, rjOutputs);
    }

protected:
    void writeMoreAndAppend(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        uint16_t length) const override {
        lengthVector->setValue<int64_t>(0, length);
        auto multiplicity =
            rjOutputs->ptrCast<AllSPMultiplicitiesOutputs>()->multiplicities->getTargetMultiplicity(
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
    explicit VarLenAndAllSPOutputWriterPaths(main::ClientContext* context, RJOutputs* rjOutputs,
        uint16_t lowerBound, uint16_t upperBound)
        : RJOutputWriter(context, rjOutputs), lowerBound{lowerBound}, upperBound{upperBound} {
        lengthVector =
            std::make_unique<ValueVector>(LogicalType::INT64(), context->getMemoryManager());
        lengthVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(lengthVector.get());
        pathNodeIDsVector = std::make_unique<ValueVector>(
            LogicalType::LIST(LogicalType::INTERNAL_ID()), context->getMemoryManager());
        pathNodeIDsVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(pathNodeIDsVector.get());
    }

    void write(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID) const override {
        VarLenOrAllSPPathsOutputs* allSpOutputs = rjOutputs->ptrCast<VarLenOrAllSPPathsOutputs>();
        PathVectorWriter writer(pathNodeIDsVector.get());
        auto firstParent = allSpOutputs->parentPtrs->getCurFixedParentPtrs()[dstNodeID.offset].load(
            std::memory_order_relaxed);
        if (firstParent == nullptr) {
            // This case should only run for variable length joins.
            dstNodeIDVector->setValue<common::nodeID_t>(0, dstNodeID);
            lengthVector->setValue<int64_t>(0, 0);
            writer.beginWritingNewPath(0);
            fTable.append(vectors);
            return;
        }
        dstNodeIDVector->setValue<common::nodeID_t>(0, dstNodeID);
        lengthVector->setValue<int64_t>(0, firstParent->getIter());
        std::vector<ParentIterAndNextPtr*> curPath;
        curPath.push_back(firstParent);
        auto backtracking = false;
        while (!curPath.empty()) {
            auto top = curPath[curPath.size() - 1];
            auto topNodeID = top->getNodeID();
            if (topNodeID == allSpOutputs->sourceNodeID) {
                auto curPathSize = curPath.size();
                writer.beginWritingNewPath(curPathSize);
                while (writer.nextPathPos > 0) {
                    writer.addNewNodeID(
                        curPath[curPathSize - (writer.nextPathPos + 1)]->getNodeID());
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
                    ((curPath.size() == 1 && top->getNextPtr()->getIter() >= lowerBound) ||
                        top->getNextPtr()->getIter() == top->getIter())) {
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
    explicit AllSPOutputWriterPaths(main::ClientContext* context, RJOutputs* rjOutputs,
        uint16_t upperBound)
        : VarLenAndAllSPOutputWriterPaths(context, rjOutputs, 1 /* lower bound */, upperBound) {}

    bool skipWriting(common::nodeID_t dstNodeID) const override {
        auto allSpOutputsPaths = rjOutputs->ptrCast<VarLenOrAllSPPathsOutputs>();
        // For all shortest path computations, we do not output any results from source to source.
        // We also do not output any results if a destination node has not been reached.
        return dstNodeID == allSpOutputsPaths->sourceNodeID ||
               nullptr ==
                   allSpOutputsPaths->parentPtrs->getCurFixedParentPtrs()[dstNodeID.offset].load(
                       std::memory_order_relaxed);
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<AllSPOutputWriterPaths>(context, rjOutputs, upperBound);
    }
};

class VarLenOutputWriterPaths : public VarLenAndAllSPOutputWriterPaths {
public:
    explicit VarLenOutputWriterPaths(main::ClientContext* context, RJOutputs* rjOutputs,
        uint16_t lowerBound, uint16_t upperBound)
        : VarLenAndAllSPOutputWriterPaths(context, rjOutputs, lowerBound, upperBound) {}

    bool skipWriting(common::nodeID_t dstNodeID) const override {
        auto pathsOutputs = rjOutputs->ptrCast<VarLenOrAllSPPathsOutputs>();
        auto firstIterParentAndNextPtr =
            pathsOutputs->parentPtrs->getCurFixedParentPtrs()[dstNodeID.offset].load(
                std::memory_order_relaxed);
        // For variable lengths joins, we skip a destination node d in the following conditions:
        //    (i) if no path has reached d from the source, except when the lower bound is 0.
        //    (ii) the longest path that has reached d, which is stored in the iter value of the
        //    firstIterParentAndNextPtr is smaller than the lower bound.
        // We also do not output any results if a destination node has not been reached.
        if (nullptr == firstIterParentAndNextPtr) {
            return lowerBound > 0;
        } else {
            return firstIterParentAndNextPtr->getIter() < lowerBound;
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<VarLenOutputWriterPaths>(context, rjOutputs, lowerBound,
            upperBound);
    }
};

struct AllSPLengthsEdgeCompute : public EdgeCompute {
    explicit AllSPLengthsEdgeCompute(SinglePathLengthsFrontierPair* pathLengthsFrontiers,
        PathMultiplicities* multiplicities)
        : pathLengthsFrontiers{pathLengthsFrontiers}, multiplicities{multiplicities} {};

    bool edgeCompute(common::nodeID_t curNodeID, common::nodeID_t nbrID) override {
        auto nbrVal =
            pathLengthsFrontiers->pathLengths->getMaskValueFromNextFrontierFixedMask(nbrID.offset);
        // We should update the nbrID's multiplicity in 2 cases: 1) if nbrID is being visited for
        // the first time, i.e., when its value in the pathLengths frontier is
        // PathLengths::UNVISITED. Or 2) if nbrID has already been visited but in this iteration,
        // so it's value is curIter + 1.
        auto shouldUpdate = nbrVal == PathLengths::UNVISITED ||
                            nbrVal == pathLengthsFrontiers->pathLengths->getCurIter();
        if (shouldUpdate) {
            // Note: This is safe because curNodeID is in the current frontier, so its
            // shortest paths multiplicity is guaranteed to not change in the current iteration.
            multiplicities->incrementTargetMultiplicity(nbrID.offset,
                multiplicities->getBoundMultiplicity(curNodeID.offset));
        }
        return nbrVal == PathLengths::UNVISITED;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<AllSPLengthsEdgeCompute>(this->pathLengthsFrontiers,
            this->multiplicities);
    }

private:
    SinglePathLengthsFrontierPair* pathLengthsFrontiers;
    PathMultiplicities* multiplicities;
};

struct AllSPPathsEdgeCompute : public EdgeCompute {
    SinglePathLengthsFrontierPair* pathLengthsFrontiers;
    ParentPtrsAtomics* parentPtrs;
    ParentPtrsBlock* parentPtrsBlock;
    explicit AllSPPathsEdgeCompute(SinglePathLengthsFrontierPair* pathLengthsFrontiers,
        ParentPtrsAtomics* parentPtrs)
        : pathLengthsFrontiers{pathLengthsFrontiers}, parentPtrs{parentPtrs} {
        parentPtrsBlock = parentPtrs->addNewBlock();
    };

    bool edgeCompute(common::nodeID_t curNodeID, common::nodeID_t nbrID) override {
        auto nbrLen =
            pathLengthsFrontiers->pathLengths->getMaskValueFromNextFrontierFixedMask(nbrID.offset);
        // We should update the nbrID's multiplicity in 2 cases: 1) if nbrID is being visited for
        // the first time, i.e., when its value in the pathLengths frontier is
        // PathLengths::UNVISITED. Or 2) if nbrID has already been visited but in this iteration,
        // so it's value is curIter + 1.
        auto shouldUpdate = nbrLen == PathLengths::UNVISITED ||
                            nbrLen == pathLengthsFrontiers->pathLengths->getCurIter();
        if (shouldUpdate) {
            if (!parentPtrsBlock->hasSpace()) {
                parentPtrsBlock = parentPtrs->addNewBlock();
            }
            parentPtrs->addParent(pathLengthsFrontiers->curIter.load(std::memory_order_relaxed),
                parentPtrsBlock, nbrID /* child */, curNodeID /* parent */);
        }
        return nbrLen == PathLengths::UNVISITED;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<AllSPPathsEdgeCompute>(this->pathLengthsFrontiers,
            this->parentPtrs);
    }
};

/**
 * Algorithm for parallel all shortest paths computation, so all shortest paths from a source to
 * is returned for each destination. If paths are not returned, multiplicities indicate the number
 * of paths to each destination.
 */

class AllSPDestinationsAlgorithm final : public SPAlgorithm {
public:
    AllSPDestinationsAlgorithm() = default;
    AllSPDestinationsAlgorithm(const AllSPDestinationsAlgorithm& other) : SPAlgorithm{other} {}

    expression_vector getResultColumns(Binder*) const override { return getNodeIDResultColumns(); }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<AllSPDestinationsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto output = std::make_unique<AllSPMultiplicitiesOutputs>(
            sharedState->graph->getNodeTableIDAndNumNodes(), sourceNodeID,
            clientContext->getMemoryManager());
        auto outputWriter = std::make_unique<AllSPOutputWriterDsts>(clientContext, output.get());
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(output->pathLengths,
            clientContext->getMaxNumThreadForExec());
        auto edgeCompute = std::make_unique<AllSPLengthsEdgeCompute>(frontierPair.get(),
            output->multiplicities.get());
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
    }
};

class AllSPLengthsAlgorithm final : public SPAlgorithm {
public:
    AllSPLengthsAlgorithm() = default;
    AllSPLengthsAlgorithm(const AllSPLengthsAlgorithm& other) : SPAlgorithm{other} {}

    expression_vector getResultColumns(Binder* binder) const override {
        auto columns = getNodeIDResultColumns();
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
        auto outputWriter = std::make_unique<AllSPOutputWriterLengths>(clientContext, output.get());
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(output->pathLengths,
            clientContext->getMaxNumThreadForExec());
        auto edgeCompute = std::make_unique<AllSPLengthsEdgeCompute>(frontierPair.get(),
            output->multiplicities.get());
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
    }
};

class AllSPPathsAlgorithm final : public SPAlgorithm {
public:
    AllSPPathsAlgorithm() = default;
    AllSPPathsAlgorithm(const AllSPPathsAlgorithm& other) : SPAlgorithm{other} {}

    expression_vector getResultColumns(Binder* binder) const override {
        auto columns = getNodeIDResultColumns();
        columns.push_back(getLengthColumn(binder));
        columns.push_back(getPathNodeIDsColumn(binder));
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<AllSPPathsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto output = std::make_unique<VarLenOrAllSPPathsOutputs>(
            sharedState->graph->getNodeTableIDAndNumNodes(), sourceNodeID,
            clientContext->getMemoryManager());
        auto outputWriter = std::make_unique<AllSPOutputWriterPaths>(clientContext, output.get(),
            bindData->ptrCast<RJBindData>()->upperBound);
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(output->pathLengths,
            clientContext->getMaxNumThreadForExec());
        auto edgeCompute =
            std::make_unique<AllSPPathsEdgeCompute>(frontierPair.get(), output->parentPtrs.get());
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
    }
};

struct VarLenJoinsEdgeCompute : public EdgeCompute {
    DoublePathLengthsFrontierPair* doublePathLengthsFrontiers;
    ParentPtrsAtomics* parentPtrs;
    ParentPtrsBlock* parentPtrsBlock;

    VarLenJoinsEdgeCompute(DoublePathLengthsFrontierPair* doublePathLengthsFrontiers,
        ParentPtrsAtomics* parentPtrs)
        : doublePathLengthsFrontiers{doublePathLengthsFrontiers}, parentPtrs{parentPtrs} {
        parentPtrsBlock = parentPtrs->addNewBlock();
    };

    bool edgeCompute(common::nodeID_t curNodeID, common::nodeID_t nbrID) override {
        // We should always update the nbrID in variable length joins
        if (!parentPtrsBlock->hasSpace()) {
            parentPtrsBlock = parentPtrs->addNewBlock();
        }
        parentPtrs->addParent(doublePathLengthsFrontiers->curIter.load(std::memory_order_relaxed),
            parentPtrsBlock, nbrID /* child */, curNodeID /* parent */);
        return true;
    }

    std::unique_ptr<EdgeCompute> copy() override {
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
    VarLenJoinsAlgorithm() = default;
    VarLenJoinsAlgorithm(const VarLenJoinsAlgorithm& other) : RJAlgorithm(other) {}

    /*
     * Inputs include the following:
     *
     * graph::ANY
     * srcNode::NODE
     * lowerBound::INT64
     * upperBound::INT64
     * outputProperty::BOOL
     */
    std::vector<LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::NODE, LogicalTypeID::INT64, LogicalTypeID::INT64,
            LogicalTypeID::BOOL};
    }

    void bind(const expression_vector& params, Binder* binder,
        graph::GraphEntry& graphEntry) override {
        auto nodeInput = params[1];
        auto nodeOutput = bindNodeOutput(binder, graphEntry);
        auto lowerBound = ExpressionUtil::getLiteralValue<int64_t>(*params[2]);
        auto upperBound = ExpressionUtil::getLiteralValue<int64_t>(*params[3]);
        validateLowerUpperBound(lowerBound, upperBound);
        auto outputProperty = ExpressionUtil::getLiteralValue<bool>(*params[4]);
        bindData = std::make_unique<RJBindData>(nodeInput, nodeOutput, outputProperty, lowerBound,
            upperBound);
    }

    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        auto columns = getNodeIDResultColumns();
        columns.push_back(getLengthColumn(binder));
        columns.push_back(getPathNodeIDsColumn(binder));
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
        auto output =
            std::make_unique<VarLenOrAllSPPathsOutputs>(nodeTableToNumNodes, sourceNodeID, mm);
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto outputWriter = std::make_unique<VarLenOutputWriterPaths>(clientContext, output.get(),
            rjBindData->lowerBound, rjBindData->upperBound);
        auto frontierPair = std::make_unique<DoublePathLengthsFrontierPair>(nodeTableToNumNodes,
            clientContext->getMaxNumThreadForExec(), mm);
        auto edgeCompute =
            std::make_unique<VarLenJoinsEdgeCompute>(frontierPair.get(), output->parentPtrs.get());
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
    }
};

function_set VarLenJoinsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(name, std::make_unique<VarLenJoinsAlgorithm>()));
    return result;
}

function_set AllSPDestinationsFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<GDSFunction>(name, std::make_unique<AllSPDestinationsAlgorithm>()));
    return result;
}

function_set AllSPLengthsFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<GDSFunction>(name, std::make_unique<AllSPLengthsAlgorithm>()));
    return result;
}

function_set AllSPPathsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(name, std::make_unique<AllSPPathsAlgorithm>()));
    return result;
}

} // namespace function
} // namespace kuzu
