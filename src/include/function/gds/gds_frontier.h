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
        graph::NbrScanState::Chunk& results, bool fwdEdge) = 0;

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
    virtual bool beginOnTable(common::table_id_t) { return true; }

    // This function is called by each worker thread T on each node in the morsel that T grabs.
    // Does any vertex-centric work that is needed while running on the curNodeID. This function
    // should itself do the work of checking if any work should be done on the vertex or not. Note
    // that this contrasts with how EdgeCompute::edgeCompute() should be implemented, where the
    // GDSUtils helper functions call isActive on nodes to check if any work should be done for
    // the edges of a node. Instead, here GDSUtils helper functions for VertexCompute blindly run
    // the function on each node in a graph.
    virtual void vertexCompute(const graph::VertexScanState::Chunk& chunk) = 0;

    virtual std::unique_ptr<VertexCompute> copy() = 0;
};

class FrontierMorsel {
    friend class FrontierMorselDispatcher;

public:
    FrontierMorsel() {}

    bool hasNextOffset() const { return nextOffset < endOffsetExclusive; }

    common::nodeID_t getNextNodeID() { return {nextOffset++, tableID}; }

    common::offset_t getBeginOffset() const { return beginOffset; }
    common::offset_t getEndOffsetExclusive() const { return endOffsetExclusive; }

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

    common::table_id_t getTableID() const { return tableID; }

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
    virtual bool isActive(common::offset_t offset) = 0;
    virtual void setActive(std::span<const common::nodeID_t> nodeIDs) = 0;
    virtual void setActive(common::nodeID_t nodeID) = 0;

    const common::table_id_map_t<common::offset_t>& getNumNodesMap() const { return numNodesMap; }
    common::offset_t getNumNodes(common::table_id_t tableID) const {
        KU_ASSERT(numNodesMap.contains(tableID));
        return numNodesMap.at(tableID);
    }

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
    using frontier_entry_t = std::atomic<uint16_t>;

public:
    static constexpr uint16_t UNVISITED = UINT16_MAX;

    PathLengths(const common::table_id_map_t<common::offset_t>& numNodesMap,
        storage::MemoryManager* mm);

    uint16_t getMaskValueFromCurFrontier(common::offset_t offset) {
        return getCurFrontier()[offset].load(std::memory_order_relaxed);
    }
    uint16_t getMaskValueFromNextFrontier(common::offset_t offset) {
        return getNextFrontier()[offset].load(std::memory_order_relaxed);
    }

    bool isActive(common::offset_t offset) override {
        return getCurFrontier()[offset] == curIter.load(std::memory_order_relaxed) - 1;
    }

    void setActive(const std::span<const common::nodeID_t> nodeIDs) override {
        auto frontierMask = getNextFrontier();
        for (const auto nodeID : nodeIDs) {
            frontierMask[nodeID.offset].store(getCurIter(), std::memory_order_relaxed);
        }
    }
    void setActive(common::nodeID_t nodeID) override {
        getNextFrontier()[nodeID.offset].store(getCurIter(), std::memory_order_relaxed);
    }

    void incrementCurIter() { curIter.fetch_add(1, std::memory_order_relaxed); }

    void pinTableID(common::table_id_t tableID) override { pinCurFrontierTableID(tableID); }
    void pinCurFrontierTableID(common::table_id_t tableID);
    void pinNextFrontierTableID(common::table_id_t tableID);

private:
    uint16_t getCurIter() { return curIter.load(std::memory_order_relaxed); }

    frontier_entry_t* getCurFrontier() {
        auto retVal = curFrontier.load(std::memory_order_relaxed);
        KU_ASSERT(retVal != nullptr);
        return retVal;
    }

    frontier_entry_t* getNextFrontier() {
        auto retVal = nextFrontier.load(std::memory_order_relaxed);
        KU_ASSERT(retVal != nullptr);
        return retVal;
    }

    frontier_entry_t* getMaskData(common::table_id_t tableID) {
        KU_ASSERT(masks.contains(tableID));
        return reinterpret_cast<frontier_entry_t*>(masks.at(tableID)->getData());
    }

private:
    common::table_id_map_t<std::unique_ptr<storage::MemoryBuffer>> masks;
    // See FrontierPair::curIter. We keep a copy of curIter here because PathLengths stores
    // iteration numbers for vertices and uses them to identify which vertex is in the frontier.
    std::atomic<uint16_t> curIter;
    std::atomic<common::table_id_t> curTableID;
    std::atomic<frontier_entry_t*> curFrontier;
    std::atomic<frontier_entry_t*> nextFrontier;
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
        std::shared_ptr<GDSFrontier> nextFrontier, uint64_t maxThreadsForExec);

    virtual ~FrontierPair() = default;

    bool getNextRangeMorsel(FrontierMorsel& morsel) {
        return morselDispatcher.getNextRangeMorsel(morsel);
    }

    void setActiveNodesForNextIter() { hasActiveNodesForNextIter_.store(true); }
    bool hasActiveNodesForNextIter() { return hasActiveNodesForNextIter_.load(); }
    void beginNewIteration();

    virtual void initRJFromSource(common::nodeID_t source) = 0;

    virtual void beginFrontierComputeBetweenTables(common::table_id_t curFrontierTableID,
        common::table_id_t nextFrontierTableID) = 0;

    uint16_t getCurrentIter() { return curIter.load(std::memory_order_relaxed); }
    uint16_t getNextIter() { return curIter.load() + 1u; }

    GDSFrontier& getCurrentFrontierUnsafe() const { return *curFrontier; }
    GDSFrontier& getNextFrontierUnsafe() { return *nextFrontier; }

protected:
    virtual void beginNewIterationInternalNoLock() {}

protected:
    std::mutex mtx;
    // curIter is the iteration number of the algorithm and starts from 0.
    std::atomic<uint16_t> curIter;
    std::atomic<bool> hasActiveNodesForNextIter_;
    std::shared_ptr<GDSFrontier> curFrontier;
    std::shared_ptr<GDSFrontier> nextFrontier;

    FrontierMorselDispatcher morselDispatcher;
};

class SinglePathLengthsFrontierPair : public FrontierPair {
public:
    SinglePathLengthsFrontierPair(std::shared_ptr<PathLengths> pathLengths,
        uint64_t maxThreadsForExec)
        : FrontierPair(pathLengths /* curFrontier */, pathLengths /* nextFrontier */,
              maxThreadsForExec),
          pathLengths{pathLengths} {}

    PathLengths* getPathLengths() const { return pathLengths.get(); }

    void initRJFromSource(common::nodeID_t source) override;

    void beginFrontierComputeBetweenTables(common::table_id_t curTableID,
        common::table_id_t nextTableID) override;

    void beginNewIterationInternalNoLock() override { pathLengths->incrementCurIter(); }

private:
    std::shared_ptr<PathLengths> pathLengths;
};

class DoublePathLengthsFrontierPair : public FrontierPair {
public:
    DoublePathLengthsFrontierPair(std::shared_ptr<PathLengths> curFrontier,
        std::shared_ptr<PathLengths> nextFrontier, uint64_t maxThreadsForExec)
        : FrontierPair{curFrontier, nextFrontier, maxThreadsForExec} {}

    void initRJFromSource(common::nodeID_t source) override;

    void beginFrontierComputeBetweenTables(common::table_id_t curTableID,
        common::table_id_t nextTableID) override;

    void beginNewIterationInternalNoLock() override;
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
