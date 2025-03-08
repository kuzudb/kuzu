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
    void setNbrInfo(common::nodeID_t nodeID_, common::relID_t edgeID_, bool isFwd_) {
        nodeID = nodeID_;
        edgeID = edgeID_;
        isFwd = isFwd_;
    }
    common::nodeID_t getNodeID() const { return nodeID; }
    common::relID_t getEdgeID() const { return edgeID; }
    bool isFwdEdge() const { return isFwd; }

    void setNextPtr(ParentList* ptr) { next.store(ptr, std::memory_order_relaxed); }
    ParentList* getNextPtr() { return next.load(std::memory_order_relaxed); }

    void setIter(uint16_t iter_) { iter = iter_; }
    uint16_t getIter() const { return iter; }

    void setCost(double cost_) { cost = cost_; }
    double getCost() const { return cost; }

private:
    common::nodeID_t nodeID;
    common::relID_t edgeID;
    bool isFwd = true;

    uint16_t iter = UINT16_MAX;
    double cost = std::numeric_limits<double>::max();
    // Next pointer
    std::atomic<ParentList*> next;
};

class BFSGraph {
    friend class BFSGraphInitVertexCompute;
    // Data type that is allocated to max num nodes per node table.
    using parent_entry_t = std::atomic<ParentList*>;

public:
    BFSGraph(common::table_id_map_t<common::offset_t> maxOffsetMap, storage::MemoryManager* mm)
        : mm{mm} {
        for (auto& [tableID, maxOffset] : maxOffsetMap) {
            parentArray.allocate(tableID, maxOffset, mm);
        }
    }

    // This function is thread safe and should be called by a worker thread Ti to grab a block
    // of memory that Ti owns and writes to.
    ObjectBlock<ParentList>* addNewBlock();

    ParentList* getParentListHead(common::nodeID_t nodeID) {
        return parentArray.getData(nodeID.tableID)[nodeID.offset].load(std::memory_order_relaxed);
    }
    ParentList* getParentListHead(common::offset_t offset) {
        return currParentPtrs[offset].load(std::memory_order_relaxed);
    }
    void setParentList(common::nodeID_t nodeID, ParentList* parentList) {
        parentArray.getData(nodeID.tableID)[nodeID.offset].store(parentList);
    }

    // Used to track path for all shortest path & variable length path.
    void addParent(uint16_t iter, common::nodeID_t boundNodeID, common::relID_t edgeID,
        common::nodeID_t nbrNodeID, bool fwdEdge, ObjectBlock<ParentList>* block);
    // Used to track path for single shortest path. Assume each offset has at most one parent.
    void addSingleParent(uint16_t iter, common::nodeID_t boundNodeID, common::relID_t edgeID,
        common::nodeID_t nbrNodeID, bool fwdEdge, ObjectBlock<ParentList>* block);
    // Used to track path for single weighted shortest path. Assume each offset has at most one
    // parent.
    bool tryAddSingleParentWithWeight(common::nodeID_t boundNodeID, common::relID_t edgeID,
        common::nodeID_t nbrNodeID, bool fwdEdge, double weight, ObjectBlock<ParentList>* block);
    // Used to track path for all weighted shortest path.
    bool tryAddParentWithWeight(common::nodeID_t boundNodeID, common::relID_t edgeID,
        common::nodeID_t nbrNodeID, bool fwdEdge, double weight, ObjectBlock<ParentList>* block);

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
