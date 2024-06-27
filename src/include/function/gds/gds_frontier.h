#pragma once

#include <atomic>
#include <mutex>
#include "common/types/internal_id_t.h"

using namespace kuzu::common;

namespace kuzu {
namespace graph {
class Graph;
} // namespace evaluator

namespace function {

// TODO(Reviewer): Is this the right place to put this?
using lock_t = std::unique_lock<std::mutex>;

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
    table_id_t tableID = UINT64_MAX;
    offset_t beginOffset = UINT64_MAX;
    offset_t endOffsetExclusive = UINT64_MAX;
    uint64_t nextOffset = UINT64_MAX;
};

/**
 * Base interface for algorithms that can be implemented in Pregel-like vertex-centric manner.
 * More specifically, this interface mimics Ligra's edgeUpdate function. Intended to be used
 * when using the helper functions in GDSUtils.
 */
class FrontierUpdateFn {
public:
    virtual ~FrontierUpdateFn() {};
    // Does any work that is needed for the neighbor nbrID. Returns true if the neighbor should
    // be put in the next frontier. So if the implementing class has access to the next frontier
    // as a field, **do not** call setActive. Helper functions in GDSUtils will do that work.
    virtual bool edgeUpdate(nodeID_t nbrID) = 0;
};


/**
 * Interface for maintaining a frontier of nodes for GDS algorithms. The frontier is a set of
 * "active nodes" for which some computation should be done at a particular iteration of a
 * GDSAlgorithm.
 * TODO: This class should be renamed to simply Frontier after we remove the Frontier classes in
 * recursive_extend/frontier.h
 */
class
    GDSFrontier {
public:
    virtual ~GDSFrontier() {};
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

public:
    explicit Frontiers(uint64_t initialActiveNodes) {
        numNextActiveNodes.store(initialActiveNodes);
        curIter.store(UINT64_MAX);
    }
    // TODO(Reviewer): Should I do this? I'm doing it because the compiler gives me this warning
    // "warning: destructor called on non-final 'kuzu::function::PathLengthsFrontiers' that has
    // virtual functions but non-virtual destructor [-Wdelete-non-abstract-non-virtual-dtor]"
    // According to a Google search, I should add a virtual destructor but I don't fully understand
    // if the deriving class, so PathLengthsFrontiers in this case, also needs a destructor.
    virtual ~Frontiers() {};
    virtual bool getNextFrontierMorsel(RangeFrontierMorsel& frontierMorsel) = 0;
    void incrementNextActiveNodes(uint64_t i) { numNextActiveNodes.fetch_add(i); }
    void beginNewIterationOfUpdates() {
        lock_t lck{mtx};
        // If curIter is UINT64_MAX, which indicates that the iterations have not started, the
        // following line will set it to 0.
        curIter.fetch_add(1);
        numNextActiveNodes.store(0u);
        beginNewIterationOfUpdatesInternalNoLock();
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
    virtual void beginFrontierUpdatesBetweenTables(table_id_t curFrontierTableID, table_id_t nextFrontierTableID) = 0;
    virtual GDSFrontier* getCurFrontier() = 0;
    virtual GDSFrontier* getNextFrontier() = 0;

    uint64_t getNextIter() {
        // Note: If curIter is UINT64_MAX, which indicates that the iterations have not started,
        // this will return 0.
        return curIter.load() + 1;
    }
    uint64_t getNumNextActiveNodes() { return numNextActiveNodes.load(); }
    // Note: If the implementing class stores 2 frontiers, this function should swap them.
    virtual void beginNewIterationOfUpdatesInternalNoLock() {}
protected:
    std::mutex mtx;
    std::atomic<uint64_t> curIter;
    std::atomic<uint64_t> numNextActiveNodes;
};

} // namespace function
} // namespace kuzu
