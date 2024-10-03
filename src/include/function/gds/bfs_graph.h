#pragma once

#include "gds_object_manager.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace function {

// TODO(Xiyang): optimize if edgeID is not needed.
class ParentList {
public:
    ParentList() = default;

    void store(uint16_t iter_, common::nodeID_t nodeID, common::relID_t edgeID, bool isFwd) {
        iter.store(iter_, std::memory_order_relaxed);
        nodeOffset.store(nodeID.offset, std::memory_order_relaxed);
        nodeTableID.store(nodeID.tableID, std::memory_order_relaxed);
        edgeOffset.store(edgeID.offset, std::memory_order_relaxed);
        edgeTableID.store(edgeID.tableID, std::memory_order_relaxed);
        fwd_.store(isFwd, std::memory_order_relaxed);
    }

    void setNextPtr(ParentList* ptr) { next.store(ptr, std::memory_order_relaxed); }

    ParentList* getNextPtr() { return next.load(std::memory_order_relaxed); }

    uint16_t getIter() { return iter.load(std::memory_order_relaxed); }

    common::nodeID_t getNodeID() {
        return {nodeOffset.load(std::memory_order_relaxed),
            nodeTableID.load(std::memory_order_relaxed)};
    }
    common::relID_t getEdgeID() {
        return {edgeOffset.load(std::memory_order_relaxed),
            edgeTableID.load(std::memory_order_relaxed)};
    }
    bool isFwdEdge() { return fwd_.load(std::memory_order_relaxed); }

private:
    // Iteration level
    std::atomic<uint16_t> iter;
    // Node information
    std::atomic<common::offset_t> nodeOffset;
    std::atomic<common::table_id_t> nodeTableID;
    // Edge information
    std::atomic<common::offset_t> edgeOffset;
    std::atomic<common::table_id_t> edgeTableID;
    // Edge direction
    std::atomic<bool> fwd_;
    // Next pointer
    std::atomic<ParentList*> next;
};

class BFSGraph {
    static constexpr uint64_t ALL_PATHS_BLOCK_SIZE = (std::uint64_t)1 << 19;
    // Data type that is allocated to max num nodes per node table.
    using parent_entry_t = std::atomic<ParentList*>;

public:
    BFSGraph(std::unordered_map<common::table_id_t, common::offset_t> nodeTableIDAndNumNodes,
        storage::MemoryManager* mm)
        : mm{mm} {
        for (auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
            parentArray.allocate(tableID, numNodes, mm);
            auto data = parentArray.getData(tableID);
            for (uint64_t i = 0; i < numNodes; ++i) {
                // Note: We are using memory_order_relaxed here because we are assuming that
                // this code is running by a master thread which will run a memory barrier
                // before worker threads start.
                data[i].store(nullptr, std::memory_order_relaxed);
            }
        }
    }

    // This function is thread safe and should be called by a worker thread Ti to grab a block
    // of memory that Ti owns and writes to.
    ObjectBlock<ParentList>* addNewBlock() {
        std::unique_lock lck{mtx};
        auto memBlock = mm->allocateBuffer(false /* don't init to 0 */, ALL_PATHS_BLOCK_SIZE);
        blocks.push_back(
            std::make_unique<ObjectBlock<ParentList>>(std::move(memBlock), ALL_PATHS_BLOCK_SIZE));
        return blocks[blocks.size() - 1].get();
    }

    ParentList* getInitialParentAndNextPtr(common::nodeID_t nodeID) {
        return parentArray.getData(nodeID.tableID)[nodeID.offset].load(std::memory_order_relaxed);
    }

    // Warning: Make sure hasSpace has returned true on parentPtrBlock already before calling this
    // function.
    void addParent(uint16_t iter, ObjectBlock<ParentList>* parentListBlock,
        common::nodeID_t childNodeID, common::nodeID_t parentNodeID, common::relID_t edgeID,
        bool isFwd) {
        auto parentEdgePtr = parentListBlock->reserveNext();
        parentEdgePtr->store(iter, parentNodeID, edgeID, isFwd);
        auto curPtr = currParentPtrs.load(std::memory_order_relaxed);
        KU_ASSERT(curPtr != nullptr);
        // Since by default the parentPtr of each node is nullptr, that's what we start with.
        ParentList* expected = nullptr;
        while (!curPtr[childNodeID.offset].compare_exchange_strong(expected, parentEdgePtr))
            ;
        parentEdgePtr->setNextPtr(expected);
    }

    // For single shortest path, we do NOT add parent if a parent has already existed.
    void tryAddSingleParent(uint16_t iter, ObjectBlock<ParentList>* parentListBlock,
        common::nodeID_t childNodeID, common::nodeID_t parentNodeID, common::relID_t edgeID,
        bool isFwd) {
        auto parentEdgePtr = parentListBlock->reserveNext();
        parentEdgePtr->store(iter, parentNodeID, edgeID, isFwd);
        auto curPtr = currParentPtrs.load(std::memory_order_relaxed);
        ParentList* expected = nullptr;
        if (curPtr[childNodeID.offset].compare_exchange_strong(expected, parentEdgePtr)) {
            parentEdgePtr->setNextPtr(expected);
        } else {
            // Do NOT add parent and revert reserved slot.
            parentListBlock->revertLast();
        }
    }

    parent_entry_t* getCurFixedParentPtrs() {
        return currParentPtrs.load(std::memory_order_relaxed);
    }

    void pinNodeTable(common::table_id_t tableID) {
        KU_ASSERT(parentArray.contains(tableID));
        currParentPtrs.store(parentArray.getData(tableID), std::memory_order_relaxed);
    }

private:
    std::mutex mtx;
    storage::MemoryManager* mm;
    ObjectArraysMap<parent_entry_t> parentArray;
    std::atomic<parent_entry_t*> currParentPtrs;
    std::vector<std::unique_ptr<ObjectBlock<ParentList>>> blocks;
};

} // namespace function
} // namespace kuzu
