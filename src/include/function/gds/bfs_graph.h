#pragma once

#include "compute.h"
#include "gds_object_manager.h"

namespace kuzu {
namespace storage {
class MemoryManager;
}
namespace function {

// TODO(Xiyang): optimize if edgeID is not needed.
class ParentList {
public:
    void store(uint16_t iter_, common::nodeID_t nodeID_, common::relID_t edgeID_, bool isFwd_) {
        iter = iter_;
        nodeID = nodeID_;
        edgeID = edgeID_;
        isFwd = isFwd_;
    }

    void setNextPtr(ParentList* ptr) { next.store(ptr, std::memory_order_relaxed); }

    ParentList* getNextPtr() { return next.load(std::memory_order_relaxed); }

    uint16_t getIter() const { return iter; }
    common::nodeID_t getNodeID() const { return nodeID; }
    common::relID_t getEdgeID() const { return edgeID; }
    bool isFwdEdge() const { return isFwd; }

private:
    // Iteration level
    uint16_t iter = UINT16_MAX;
    common::nodeID_t nodeID;
    common::relID_t edgeID;
    bool isFwd = true;
    // Next pointer
    std::atomic<ParentList*> next;
};

class BFSGraph {
    friend class BFSGraphInitVertexCompute;
    static constexpr uint64_t BFS_GRAPH_BLOCK_SIZE = (std::uint64_t)1 << 19;
    // Data type that is allocated to max num nodes per node table.
    using parent_entry_t = std::atomic<ParentList*>;

public:
    BFSGraph(common::table_id_map_t<common::offset_t> numNodesMap, storage::MemoryManager* mm)
        : mm{mm} {
        for (auto& [tableID, numNodes] : numNodesMap) {
            parentArray.allocate(tableID, numNodes, mm);
        }
    }

    // This function is thread safe and should be called by a worker thread Ti to grab a block
    // of memory that Ti owns and writes to.
    ObjectBlock<ParentList>* addNewBlock() {
        std::unique_lock lck{mtx};
        auto memBlock = mm->allocateBuffer(false /* init to 0 */, BFS_GRAPH_BLOCK_SIZE);
        blocks.push_back(
            std::make_unique<ObjectBlock<ParentList>>(std::move(memBlock), BFS_GRAPH_BLOCK_SIZE));
        return blocks[blocks.size() - 1].get();
    }

    ParentList* getParentListHead(common::nodeID_t nodeID) {
        return parentArray.getData(nodeID.tableID)[nodeID.offset].load(std::memory_order_relaxed);
    }
    ParentList* getParentListHead(common::offset_t offset) {
        return currParentPtrs[offset].load(std::memory_order_relaxed);
    }

    void addParent(ParentList* parentPtr, common::offset_t childNodeOffset) {
        // Since by default the parentPtr of each node is nullptr, that's what we start with.
        ParentList* expected = nullptr;
        while (!currParentPtrs[childNodeOffset].compare_exchange_strong(expected, parentPtr))
            ;
        parentPtr->setNextPtr(expected);
    }

    void tryAddSingleParent(ParentList* parentPtr, common::offset_t childNodeOffset,
        ObjectBlock<ParentList>* parentListBlock) {
        ParentList* expected = nullptr;
        if (currParentPtrs[childNodeOffset].compare_exchange_strong(expected, parentPtr)) {
            parentPtr->setNextPtr(expected);
        } else {
            // Do NOT add parent and revert reserved slot.
            parentListBlock->revertLast();
        }
    }

    void pinTableID(common::table_id_t tableID) { currParentPtrs = parentArray.getData(tableID); }

private:
    std::mutex mtx;
    storage::MemoryManager* mm;
    ObjectArraysMap<parent_entry_t> parentArray;
    parent_entry_t* currParentPtrs = nullptr;
    std::vector<std::unique_ptr<ObjectBlock<ParentList>>> blocks;
};

class BFSGraphInitVertexCompute : public VertexCompute {
public:
    explicit BFSGraphInitVertexCompute(BFSGraph& bfsGraph) : bfsGraph{bfsGraph} {}

    bool beginOnTable(common::table_id_t tableID) override {
        bfsGraph.pinTableID(tableID);
        return true;
    }

    void vertexCompute(common::offset_t startOffset, common::offset_t endOffset,
        common::table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            bfsGraph.currParentPtrs[i].store(nullptr);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<BFSGraphInitVertexCompute>(bfsGraph);
    }

private:
    BFSGraph& bfsGraph;
};

} // namespace function
} // namespace kuzu
