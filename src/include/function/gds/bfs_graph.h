#pragma once

#include "compute.h"
#include "density_state.h"
#include "gds_object_manager.h"

namespace kuzu {
namespace storage {
class MemoryManager;
}
namespace processor {
struct  ExecutionContext;
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
    using bfs_graph_parent_entry_t = std::atomic<ParentList*>;

public:
    BFSGraph(common::table_id_map_t<common::offset_t> maxOffsetMap, storage::MemoryManager* mm)
        : maxOffsetMap{maxOffsetMap}, mm{mm} {}

    void initDenseObjects(processor::ExecutionContext* context, graph::Graph* graph);
    // Pin data structure to given tableID
    void pinTableID(common::table_id_t tableID);

    ParentList* getParentListHeadDense(common::nodeID_t nodeID) {
        KU_ASSERT(state == GDSDensityState::DENSE && curDenseData);
        return denseObjects.getData(nodeID.tableID)[nodeID.offset].load(std::memory_order_relaxed);
    }
    ParentList* getParentListHeadDense(common::offset_t offset) {
        KU_ASSERT(state == GDSDensityState::DENSE && curDenseData);
        return curDenseData[offset].load(std::memory_order_relaxed);
    }
    void setParentList(common::nodeID_t nodeID, ParentList* parentList) {
        KU_ASSERT(state == GDSDensityState::DENSE);
        denseObjects.getData(nodeID.tableID)[nodeID.offset].store(parentList);
    }

    // This function should be called by a worker thread Ti to grab a block of memory that
    // Ti owns and writes to.
    ObjectBlock<ParentList>* addNewBlock();

    void addParentSparse(uint16_t iter, common::nodeID_t boundNodeID, common::relID_t edgeID,
        common::nodeID_t nbrNodeID, bool fwdEdge, ObjectBlock<ParentList>* block);
    // Used to track path for all shortest path & variable length path.
    void addParentDense(uint16_t iter, common::nodeID_t boundNodeID, common::relID_t edgeID,
        common::nodeID_t nbrNodeID, bool fwdEdge, ObjectBlock<ParentList>* block);
    // Used to track path for single shortest path. Assume each offset has at most one parent.
    void addSingleParentDense(uint16_t iter, common::nodeID_t boundNodeID, common::relID_t edgeID,
        common::nodeID_t nbrNodeID, bool fwdEdge, ObjectBlock<ParentList>* block);
    // Used to track path for single weighted shortest path. Assume each offset has at most one
    // parent.
    bool tryAddSingleParentWithWeightDense(common::nodeID_t boundNodeID, common::relID_t edgeID,
        common::nodeID_t nbrNodeID, bool fwdEdge, double weight, ObjectBlock<ParentList>* block);
    // Used to track path for all weighted shortest path.
    bool tryAddParentWithWeightDense(common::nodeID_t boundNodeID, common::relID_t edgeID,
        common::nodeID_t nbrNodeID, bool fwdEdge, double weight, ObjectBlock<ParentList>* block);

    void switchToDense();

private:
    common::table_id_map_t<common::offset_t> maxOffsetMap;
    // Density state
    GDSDensityState state = GDSDensityState::SPARSE;
    // Dense data
    GDSDenseObjectManager<bfs_graph_parent_entry_t> denseObjects;
    bfs_graph_parent_entry_t* curDenseData = nullptr;
    // Sparse Data
    GDSSpareObjectManager<bfs_graph_parent_entry_t> sparseObjects;
    std::unordered_map<common::offset_t, bfs_graph_parent_entry_t>* curSparseData = nullptr;
    // Manage allocated memory
    std::mutex mtx;
    storage::MemoryManager* mm;
    std::vector<std::unique_ptr<ObjectBlock<ParentList>>> blocks;
};

class BFSGraphInitVertexCompute : public VertexCompute {
public:
    explicit BFSGraphInitVertexCompute(BFSGraph& bfsGraph) : bfsGraph{bfsGraph} {}

    bool beginOnTable(common::table_id_t tableID) override {
        KU_ASSERT(bfsGraph.state == GDSDensityState::DENSE);
        bfsGraph.pinTableID(tableID);
        return true;
    }

    void vertexCompute(common::offset_t startOffset, common::offset_t endOffset,
        common::table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            bfsGraph.curDenseData[i].store(nullptr);
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
