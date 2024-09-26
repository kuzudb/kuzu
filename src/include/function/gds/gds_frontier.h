#pragma once

#include <atomic>
#include <mutex>

#include "common/types/types.h"

using namespace kuzu::common;

namespace kuzu {
namespace graph {
class Graph;
} // namespace graph

namespace function {

/**
 * Base interface for algorithms that can be implemented in Pregel-like vertex-centric manner or
 * more specifically Ligra's edgeCompute (called edgeUpdate in Ligra paper) function. Intended to be
 * passed to the helper functions in GDSUtils that parallelize such Pregel-like computations.
 */
class EdgeCompute {
public:
    virtual ~EdgeCompute() = default;

    // Does any work that is needed while extending the (curNodeID, nbrID) edge. curNodeID is the
    // nodeID that is in the current frontier and currently executing. Returns true if the neighbor
    // should be put in the next frontier. So if the implementing class has access to the next
    // frontier as a field, **do not** call setActive. Helper functions in GDSUtils will do that
    // work.
    virtual bool edgeCompute(nodeID_t curNodeID, nodeID_t nbrID) = 0;

    virtual std::unique_ptr<EdgeCompute> copy() = 0;
};

class VertexCompute {
public:
    virtual ~VertexCompute() = default;

    // This function is called once on the "main" copy of VertexCompute in the
    // GDSUtils::runVertexComputeIteration function. runVertexComputeIteration loops through
    // each node table T on the graph on which vertexCompute should run and then before
    // parallelizing the computation on T calls this function.
    virtual void beginOnTable(table_id_t) {}

    // This function is called by each worker thread T on each node in the morsel that T grabs.
    // Does any vertex-centric work that is needed while running on the curNodeID. This function
    // should itself do the work of checking if any work should be done on the vertex or not. Note
    // that this contrasts with how EdgeCompute::edgeCompute() should be implemented, where the
    // GDSUtils helper functions call isActive on nodes to check if any work should be done for
    // the edges of a node. Instead, here GDSUtils helper functions for VertexCompute blindly run
    // the function on each node in a graph.
    virtual void vertexCompute(nodeID_t curNodeID) = 0;

    // This function is called by each worker thread T once at the end of
    // GDSUtils::runVertexComputeIteration().
    virtual void finalizeWorkerThread() {}

    virtual std::unique_ptr<VertexCompute> copy() = 0;
};

class FrontierMorsel {
    friend class FrontierMorselDispatcher;

public:
    FrontierMorsel() {}

    bool hasNextOffset() const { return nextOffset < endOffsetExclusive; }

    nodeID_t getNextNodeID() { return {nextOffset++, tableID}; }

protected:
    void initMorsel(table_id_t _tableID, offset_t _beginOffset, offset_t _endOffsetExclusive) {
        tableID = _tableID;
        beginOffset = _beginOffset;
        endOffsetExclusive = _endOffsetExclusive;
        nextOffset = beginOffset;
    }

private:
    table_id_t tableID = INVALID_TABLE_ID;
    offset_t beginOffset = INVALID_OFFSET;
    offset_t endOffsetExclusive = INVALID_OFFSET;
    offset_t nextOffset = INVALID_OFFSET;
};

class FrontierMorselDispatcher {
    static constexpr uint64_t MIN_FRONTIER_MORSEL_SIZE = 512;
    // Note: MIN_NUMBER_OF_FRONTIER_MORSELS is the minimum number of morsels we aim to have but we
    // can have fewer than this. See the beginFrontierComputeBetweenTables to see the actual
    // morselSize computation for details.
    static constexpr uint64_t MIN_NUMBER_OF_FRONTIER_MORSELS = 128;

public:
    explicit FrontierMorselDispatcher(uint64_t _maxThreadsForExec) : morselSize(UINT64_MAX) {
        maxThreadsForExec.store(_maxThreadsForExec);
    }

    void init(table_id_t _tableID, common::offset_t _numOffsets);

    bool getNextRangeMorsel(FrontierMorsel& frontierMorsel);

private:
    std::atomic<uint64_t> maxThreadsForExec;
    std::atomic<table_id_t> tableID;
    std::atomic<uint64_t> numOffsets;
    std::atomic<offset_t> nextOffset;
    uint64_t morselSize;
};

/**
 * Interface for maintaining a frontier of nodes for GDS algorithms. The frontier is a set of
 * "active nodes" for which some computation should be done at a particular iteration of a
 * GDSAlgorithm.
 * TODO(Semih/Xiyang): This class should be renamed to simply Frontier after we remove the Frontier
 * classes in recursive_extend/frontier.h
 */
class GDSFrontier {
public:
    virtual ~GDSFrontier() = default;
    virtual bool isActive(nodeID_t nodeID) = 0;
    virtual void setActive(nodeID_t nodeID) = 0;
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }
};

/**
 * Base class for maintaining a current and a next GDSFrontier of nodes for GDS algorithms. At any
 * point in time, maintains the current iteration curIter the algorithm is in and the number of
 * active nodes that have been set for the next iteration. This information can be used
 * to determine if the algorithm has converged or not.
 *
 * All functions supported in this base interface are thread-safe.
 */
class FrontierPair {
    friend class FrontierTask;

public:
    explicit FrontierPair(std::shared_ptr<GDSFrontier> curFrontier,
        std::shared_ptr<GDSFrontier> nextFrontier, uint64_t initialActiveNodes,
        uint64_t maxThreadsForExec);

    virtual ~FrontierPair() = default;

    virtual bool getNextRangeMorsel(FrontierMorsel& frontierMorsel) = 0;

    void incrementApproxActiveNodesForNextIter(uint64_t i) {
        numApproxActiveNodesForNextIter.fetch_add(i);
    }
    void beginNewIteration();

    virtual void initRJFromSource(nodeID_t source) = 0;

    virtual void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) = 0;

    idx_t getNextIter() { return curIter.load() + 1u; }

    bool hasActiveNodesForNextLevel() { return numApproxActiveNodesForNextIter.load() > 0; }

    // Note: If the implementing class stores 2 frontierPair, this function should swap them.
    virtual void beginNewIterationInternalNoLock() {}

protected:
    std::mutex mtx;
    // curIter is the iteration number of the algorithm and starts from 0.
    std::atomic<uint16_t> curIter;
    std::atomic<uint64_t> numApproxActiveNodesForCurIter;
    // Note: This number is not guaranteed to be accurate. However, if it is > 0, then there is at
    // least one active node for the next frontier. It may not be accurate because there can be
    // double counting. Each thread will locally increment this number based on the number of times
    // they set a node active for the next frontier. But both within a single thread's counting and
    // across threads, the same node can be set active. So do not make any reliance on the accuracy
    // of this value.
    std::atomic<uint64_t> numApproxActiveNodesForNextIter;
    std::shared_ptr<GDSFrontier> curFrontier;
    std::shared_ptr<GDSFrontier> nextFrontier;
    uint64_t maxThreadsForExec;
};

} // namespace function
} // namespace kuzu
