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

class RangeFrontierMorsel {
    friend class PathLengthsFrontiers;

public:
    RangeFrontierMorsel() {}

    bool hasNextVertex() const { return nextOffset < endOffsetExclusive; }

    nodeID_t getNextVertex() { return {nextOffset++, tableID}; }

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

/**
 * Base interface for algorithms that can be implemented in Pregel-like vertex-centric manner.
 * More specifically, this interface mimics Ligra's edgeUpdate function (though we call it
 * edgeCompute). Intended to be used when using the helper functions in GDSUtils. The name
 * FrontierCompute comes from vertex.compute() or edge.compute() functions of these systems.
 */
class FrontierCompute {
public:
    virtual ~FrontierCompute() = default;
    // Does any work that is needed for the neighbor nbrID. Returns true if the neighbor should
    // be put in the next frontier. So if the implementing class has access to the next frontier
    // as a field, **do not** call setActive. Helper functions in GDSUtils will do that work.
    virtual bool edgeCompute(nodeID_t nbrID) = 0;
};

/**
 * Interface for maintaining a frontier of nodes for GDS algorithms. The frontier is a set of
 * "active nodes" for which some computation should be done at a particular iteration of a
 * GDSAlgorithm.
 * TODO: This class should be renamed to simply Frontier after we remove the Frontier classes in
 * recursive_extend/frontier.h
 */
class GDSFrontier {
public:
    virtual ~GDSFrontier() = default;
    virtual bool isActive(nodeID_t nodeID) = 0;
    virtual void setActive(nodeID_t nodeID) = 0;
};

/**
 * Base class for maintaining a current and a next GDSFrontier of nodes for GDS algorithms. At any
 * point in time, maintains the current iteration curIter the algorithm is in and the number of
 * active nodes that have been set for the next iteration. This information can be used
 * to determine if the algorithm has converged or not.
 *
 * All functions supported in this base interface are thread-safe.
 */
class Frontiers {
    friend class GDSTask;

public:
    explicit Frontiers(GDSFrontier* curFrontier, GDSFrontier* nextFrontier,
        uint64_t initialActiveNodes)
        : curFrontier{curFrontier}, nextFrontier{nextFrontier} {
        numApproxActiveNodesForNextIter.store(initialActiveNodes);
        curIter.store(INVALID_IDX);
    }
    virtual ~Frontiers() = default;
    virtual bool getNextFrontierMorsel(RangeFrontierMorsel& frontierMorsel) = 0;
    void incrementApproxActiveNodesForNextIter(uint64_t i) {
        numApproxActiveNodesForNextIter.fetch_add(i);
    }
    void beginNewIteration() {
        std::unique_lock<std::mutex> lck{mtx};
        // If curIter is INVALID_IDX (which should be UINT32_MAX), which indicates that the
        // iterations have not started, the following line will set it to 0.
        curIter.fetch_add(1u);
        numApproxActiveNodesForNextIter.store(0u);
        beginNewIterationInternalNoLock();
    }
    // When performing computations on multi-label graphs, it may be beneficial to fix a single
    // node table of nodes in the current frontier and a single node table of nodes for the next
    // frontier. That is because algorithms will generally perform extensions using a single
    // relationship table at a time, and each relationship table R is between a single source node
    // table S and a single destination node table T. Therefore, generally during execution the
    // algorithm will need to check only the active S nodes in current frontier and update the
    // active statuses of only the T nodes in the next frontier. The information that the algorithm
    // is beginning and S-to-T extensions can be given to the frontiers to possibly avoid them
    // doing lookups of S and T-related data structures, e.g., masks, internally.
    // Note that ideally a similar function, say called "fixNodeTable", could be in GDSFrontier,
    // since Frontiers has two GDSFrontier instances in it. However, some implementations of
    // Frontiers can use a single GDSFrontier to mimic two frontiers (e.g., identifying nodes
    // in current or next frontier by specific mask values. For such implementations we would have
    // to call fixNodeTable(S) and fixNodeTable(T) on the same object, which could overwrite
    // each other. That is why this function is put in the Frontiers class.
    virtual void beginFrontierComputeBetweenTables(table_id_t curFrontierTableID,
        table_id_t nextFrontierTableID) = 0;
    idx_t getNextIter() {
        // Note: If curIter is INVALID_IDX (which should be UINT32_MAX), which indicates that the
        // iterations have not started, this will return 0.
        return curIter.load() + 1u;
    }
    bool hasActiveNodesForNextIter() { return numApproxActiveNodesForNextIter.load() > 0; }
    // Note: If the implementing class stores 2 frontiers, this function should swap them.
    virtual void beginNewIterationInternalNoLock() {}

protected:
    std::mutex mtx;
    std::atomic<idx_t> curIter;
    // Note: This number is not guaranteed to accurate. However if it is > 0, then there is at least
    // one active node for the next frontier. It may not be accurate because there ca be double
    // counting. Each thread will locally increment this number based on the number of times
    // they set a node active for the next frontier. But both within a single thread's counting and
    // across threads, the same node can be set active. So do not make any reliance on the accuracy
    // of this value.
    std::atomic<uint64_t> numApproxActiveNodesForNextIter;
    GDSFrontier* curFrontier;
    GDSFrontier* nextFrontier;
};

} // namespace function
} // namespace kuzu
