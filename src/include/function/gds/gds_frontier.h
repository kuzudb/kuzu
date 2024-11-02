#pragma once

#include <atomic>
#include <mutex>

#include "common/types/types.h"
#include "graph/graph.h"
#include "processor/operator/gds_call_shared_state.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace function {

/**
 * Base interface for algorithms that can be implemented in Pregel-like vertex-centric manner or
 * more specifically Ligra's edgeCompute (called edgeUpdate in Ligra paper) function. Intended to be
 * passed to the helper functions in GDSUtils that parallelize such Pregel-like computations.
 */
class EdgeCompute {
public:
    virtual ~EdgeCompute() = default;

    // Does any work that is needed while extending the (boundNodeID, nbrNodeID, edgeID) edge.
    // boundNodeID is the nodeID that is in the current frontier and currently executing.
    // Returns a list of neighbors which should be put in the next frontier.
    // So if the implementing class has access to the next frontier as a field,
    // **do not** call setActive. Helper functions in GDSUtils will do that work.
    virtual std::vector<common::nodeID_t> edgeCompute(common::nodeID_t boundNodeID,
        graph::GraphScanState::Chunk& results, bool fwdEdge) = 0;

    virtual void resetSingleThreadState() {}

    virtual bool terminate(processor::NodeOffsetMaskMap&) { return false; }

    virtual std::unique_ptr<EdgeCompute> copy() = 0;
};

class VertexCompute {
public:
    virtual ~VertexCompute() = default;

    // This function is called once on the "main" copy of VertexCompute in the
    // GDSUtils::runVertexComputeIteration function. runVertexComputeIteration loops through
    // each node table T on the graph on which vertexCompute should run and then before
    // parallelizing the computation on T calls this function.
    virtual void beginOnTable(common::table_id_t) {}

    // This function is called by each worker thread T on each node in the morsel that T grabs.
    // Does any vertex-centric work that is needed while running on the curNodeID. This function
    // should itself do the work of checking if any work should be done on the vertex or not. Note
    // that this contrasts with how EdgeCompute::edgeCompute() should be implemented, where the
    // GDSUtils helper functions call isActive on nodes to check if any work should be done for
    // the edges of a node. Instead, here GDSUtils helper functions for VertexCompute blindly run
    // the function on each node in a graph.
    virtual void vertexCompute(common::nodeID_t curNodeID) = 0;

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

    common::nodeID_t getNextNodeID() { return {nextOffset++, tableID}; }

protected:
    void initMorsel(common::table_id_t _tableID, common::offset_t _beginOffset,
        common::offset_t _endOffsetExclusive) {
        tableID = _tableID;
        beginOffset = _beginOffset;
        endOffsetExclusive = _endOffsetExclusive;
        nextOffset = beginOffset;
    }

private:
    common::table_id_t tableID = common::INVALID_TABLE_ID;
    common::offset_t beginOffset = common::INVALID_OFFSET;
    common::offset_t endOffsetExclusive = common::INVALID_OFFSET;
    common::offset_t nextOffset = common::INVALID_OFFSET;
};

class FrontierMorselDispatcher {
    static constexpr uint64_t MIN_FRONTIER_MORSEL_SIZE = 512;
    // Note: MIN_NUMBER_OF_FRONTIER_MORSELS is the minimum number of morsels we aim to have but we
    // can have fewer than this. See the beginFrontierComputeBetweenTables to see the actual
    // morselSize computation for details.
    static constexpr uint64_t MIN_NUMBER_OF_FRONTIER_MORSELS = 128;

public:
    explicit FrontierMorselDispatcher(uint64_t _maxThreadsForExec);

    void init(common::table_id_t _tableID, common::offset_t _numOffsets);

    bool getNextRangeMorsel(FrontierMorsel& frontierMorsel);

private:
    std::atomic<uint64_t> maxThreadsForExec;
    std::atomic<common::table_id_t> tableID;
    std::atomic<common::offset_t> numOffsets;
    std::atomic<common::offset_t> nextOffset;
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
    explicit GDSFrontier(const common::table_id_map_t<common::offset_t>& numNodesMap)
        : numNodesMap{numNodesMap} {}

    virtual ~GDSFrontier() = default;
    virtual void pinTableID(common::table_id_t tableID) = 0;
    // isActive assumes a tableID has been pinned.
    virtual bool isActive(common::offset_t nodeID) = 0;
    virtual void setActive(std::span<const common::nodeID_t> nodeIDs) = 0;
    virtual void setActive(common::nodeID_t nodeID) = 0;

    const common::table_id_map_t<common::offset_t>& getNumNodesMap() const { return numNodesMap; }

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }

protected:
    // We do not need to make nodeTableIDAndNumNodesMap and masks atomic because they should only
    // be accessed by functions that are called by the "master GDS thread" (so not accessed inside
    // the parallel functions in GDSUtils, which are called by other "worker threads").
    common::table_id_map_t<common::offset_t> numNodesMap;
};

/**
 * A GDSFrontier implementation that keeps the lengths of the paths from a source node to
 * destination nodes. This is a light-weight implementation that can keep lengths up to and
 * including UINT16_MAX - 1. The length stored for the source node is 0. Length UINT16_MAX is
 * reserved for marking nodes that are not visited. The lengths stored per node are effectively the
 * iteration numbers that a node is visited. For example, if the running computation is shortest
 * path, then the length stored is the shortest path length.
 *
 * Note: This class can be used to represent both the current and next frontierPair for shortest
 * paths computations, which have the guarantee that a vertex is part of only one frontier level.
 * Specifically, at iteration i of the shortest path algorithm (iterations start from 0), nodes with
 * mask values i are in the current frontier. Nodes with any other values are not in the frontier.
 * Similarly at iteration i setting a node u active sets its mask value to i+1. To enable this
 * usage, this class contains functions to expose two frontierPair to users, e.g.,
 * getMaskValueFromCur/NextFrontierFixedMask. In the case of shortest path computations, using this
 * class to represent two frontierPair should be faster or take less space than keeping two separate
 * frontierPair.
 *
 * However, this is not necessary and the caller can also use this to represent a single frontier.
 */
class PathLengths : public GDSFrontier {
    friend class SingleSPDestinationsEdgeCompute;

public:
    static constexpr uint16_t UNVISITED = UINT16_MAX;

    PathLengths(const common::table_id_map_t<common::offset_t>& numNodesMap,
        storage::MemoryManager* mm);

    uint16_t getMaskValueFromCurFrontierFixedMask(common::offset_t nodeOffset) {
        return getCurFrontierFixedMask()[nodeOffset].load(std::memory_order_relaxed);
    }

    uint16_t getMaskValueFromNextFrontierFixedMask(common::offset_t nodeOffset) {
        return getNextFrontierFixedMask()[nodeOffset].load(std::memory_order_relaxed);
    }

    void pinTableID(common::table_id_t tableID) override { fixCurFrontierNodeTable(tableID); }

    bool isActive(common::offset_t offset) override {
        return getCurFrontierFixedMask()[offset] == curIter.load(std::memory_order_relaxed) - 1;
    }

    void setActive(const std::span<const common::nodeID_t> nodeIDs) override {
        auto frontierMask = getNextFrontierFixedMask();
        for (const auto nodeID : nodeIDs) {
            frontierMask[nodeID.offset].store(curIter.load(std::memory_order_relaxed),
                std::memory_order_relaxed);
        }
    }

    void setActive(common::nodeID_t nodeID) override {
        getNextFrontierFixedMask()[nodeID.offset].store(curIter.load(std::memory_order_relaxed),
            std::memory_order_relaxed);
    }

    void incrementCurIter() { curIter.fetch_add(1, std::memory_order_relaxed); }

    void fixCurFrontierNodeTable(common::table_id_t tableID);

    void fixNextFrontierNodeTable(common::table_id_t tableID);

    uint64_t getNumNodesInCurFrontierFixedNodeTable() {
        KU_ASSERT(curFrontierFixedMask.load(std::memory_order_relaxed) != nullptr);
        return maxNodesInCurFrontierFixedMask.load(std::memory_order_relaxed);
    }

    uint16_t getCurIter() { return curIter.load(std::memory_order_relaxed); }

private:
    std::atomic<uint16_t>* getCurFrontierFixedMask() {
        auto retVal = curFrontierFixedMask.load(std::memory_order_relaxed);
        KU_ASSERT(retVal != nullptr);
        return retVal;
    }

    std::atomic<uint16_t>* getNextFrontierFixedMask() {
        auto retVal = nextFrontierFixedMask.load(std::memory_order_relaxed);
        KU_ASSERT(retVal != nullptr);
        return retVal;
    }

private:
    common::table_id_map_t<std::unique_ptr<storage::MemoryBuffer>> masks;
    // See FrontierPair::curIter. We keep a copy of curIter here because PathLengths stores
    // iteration numbers for vertices and uses them to identify which vertex is in the frontier.
    std::atomic<uint16_t> curIter;
    std::atomic<common::table_id_t> curTableID;
    std::atomic<uint64_t> maxNodesInCurFrontierFixedMask;
    std::atomic<std::atomic<uint16_t>*> curFrontierFixedMask;
    std::atomic<std::atomic<uint16_t>*> nextFrontierFixedMask;
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
    FrontierPair(std::shared_ptr<GDSFrontier> curFrontier,
        std::shared_ptr<GDSFrontier> nextFrontier, uint64_t initialActiveNodes,
        uint64_t maxThreadsForExec);

    virtual ~FrontierPair() = default;

    virtual bool getNextRangeMorsel(FrontierMorsel& frontierMorsel) = 0;

    void incrementApproxActiveNodesForNextIter(uint64_t i) {
        numApproxActiveNodesForNextIter.fetch_add(i);
    }
    void beginNewIteration();

    virtual void initRJFromSource(common::nodeID_t source) = 0;

    virtual void beginFrontierComputeBetweenTables(common::table_id_t curFrontierTableID,
        common::table_id_t nextFrontierTableID) = 0;

    uint16_t getCurrentIter() { return curIter.load(std::memory_order_relaxed); }
    uint16_t getNextIter() { return curIter.load() + 1u; }
    GDSFrontier& getCurrentFrontierUnsafe() const { return *curFrontier; }

    GDSFrontier& getNextFrontierUnsafe() { return *nextFrontier; }

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

class SinglePathLengthsFrontierPair : public FrontierPair {
    friend class AllSPDestinationsEdgeCompute;
    friend class AllSPPathsEdgeCompute;
    friend class SingleSPDestinationsEdgeCompute;
    friend class SingleSPPathsEdgeCompute;

public:
    SinglePathLengthsFrontierPair(std::shared_ptr<PathLengths> pathLengths,
        uint64_t maxThreadsForExec)
        : FrontierPair(pathLengths /* curFrontier */, pathLengths /* nextFrontier */,
              1 /* initial num active nodes */, maxThreadsForExec),
          pathLengths{pathLengths}, morselDispatcher(maxThreadsForExec) {}

    bool getNextRangeMorsel(FrontierMorsel& frontierMorsel) override;

    void initRJFromSource(common::nodeID_t source) override;

    void beginFrontierComputeBetweenTables(common::table_id_t curFrontierTableID,
        common::table_id_t nextFrontierTableID) override;

    void beginNewIterationInternalNoLock() override { pathLengths->incrementCurIter(); }

private:
    std::shared_ptr<PathLengths> pathLengths;
    FrontierMorselDispatcher morselDispatcher;
};

class DoublePathLengthsFrontierPair : public FrontierPair {
public:
    DoublePathLengthsFrontierPair(common::table_id_map_t<common::offset_t> numNodesMap,
        uint64_t maxThreadsForExec, storage::MemoryManager* mm);

    bool getNextRangeMorsel(FrontierMorsel& frontierMorsel) override;

    void initRJFromSource(common::nodeID_t source) override;

    void beginFrontierComputeBetweenTables(common::table_id_t curFrontierTableID,
        common::table_id_t nextFrontierTableID) override;

    void beginNewIterationInternalNoLock() override {
        curFrontier->ptrCast<PathLengths>()->incrementCurIter();
        nextFrontier->ptrCast<PathLengths>()->incrementCurIter();
    }

private:
    std::unique_ptr<FrontierMorselDispatcher> morselDispatcher;
};

class SPEdgeCompute : public EdgeCompute {
public:
    explicit SPEdgeCompute(SinglePathLengthsFrontierPair* frontierPair)
        : frontierPair{frontierPair}, numNodesReached{0} {}

    void resetSingleThreadState() override { numNodesReached = 0; }

    bool terminate(processor::NodeOffsetMaskMap& maskMap) override;

protected:
    SinglePathLengthsFrontierPair* frontierPair;
    // States that should be only modified with single thread
    common::offset_t numNodesReached;
};

} // namespace function
} // namespace kuzu
