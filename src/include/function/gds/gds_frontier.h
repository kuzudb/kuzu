#pragma once

#include <atomic>
#include <mutex>

#include "compute.h"
#include <span>

namespace kuzu {
namespace storage {
class MemoryManager;
}

namespace processor {
struct ExecutionContext;
}

namespace function {

class FrontierMorsel {
public:
    FrontierMorsel() = default;

    common::offset_t getBeginOffset() const { return beginOffset; }
    common::offset_t getEndOffset() const { return endOffset; }

    void init(common::offset_t beginOffset_, common::offset_t endOffset_) {
        beginOffset = beginOffset_;
        endOffset = endOffset_;
    }

private:
    common::offset_t beginOffset = common::INVALID_OFFSET;
    common::offset_t endOffset = common::INVALID_OFFSET;
};

class KUZU_API FrontierMorselDispatcher {
    static constexpr uint64_t MIN_FRONTIER_MORSEL_SIZE = 512;
    // Note: MIN_NUMBER_OF_FRONTIER_MORSELS is the minimum number of morsels we aim to have but we
    // can have fewer than this. See the beginFrontierComputeBetweenTables to see the actual
    // morselSize computation for details.
    static constexpr uint64_t MIN_NUMBER_OF_FRONTIER_MORSELS = 128;

public:
    explicit FrontierMorselDispatcher(uint64_t maxThreads);

    void init(common::offset_t _maxOffset);

    bool getNextRangeMorsel(FrontierMorsel& frontierMorsel);

private:
    common::offset_t maxOffset;
    std::atomic<common::offset_t> nextOffset;
    uint64_t maxThreads;
    uint64_t morselSize;
};

/*
 * Sparse frontier keeps a small frontier.
 * If enabled, this frontier represents a complete frontier.
 * Otherwise, complete frontier is larger than this sparse frontier.
 * */
class KUZU_API SparseFrontier {
public:
    SparseFrontier() : SparseFrontier{DEFAULT_SAMPLE_SIZE} {}
    explicit SparseFrontier(uint64_t sampleSize)
        : sampleSize{sampleSize}, enabled_{true}, curTableID{common::INVALID_TABLE_ID},
          curOffsetSet{nullptr} {}

    void disable() { enabled_ = false; }
    void resetState() {
        enabled_ = true;
        curOffsetSet = nullptr;
        tableIDToOffsetMap.clear();
    }
    bool enabled() const { return enabled_; }

    void pinTableID(common::table_id_t tableID);

    common::table_id_t getTableID() const { return curTableID; }
    const std::unordered_set<common::offset_t>& getOffsetSet() const { return *curOffsetSet; }

    void addNode(common::offset_t offset);
    void addNodes(const std::vector<common::nodeID_t> nodeIDs);
    void checkSampleSize();

    void mergeLocalFrontier(const SparseFrontier& localFrontier);
    void mergeSparseFrontier(const SparseFrontier& other);

private:
    static constexpr uint16_t DEFAULT_SAMPLE_SIZE = 20;
    uint64_t sampleSize;

    std::mutex mtx;
    bool enabled_;
    std::unordered_map<common::table_id_t, std::unordered_set<common::offset_t>> tableIDToOffsetMap;
    common::table_id_t curTableID;
    std::unordered_set<common::offset_t>* curOffsetSet;
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
class KUZU_API PathLengths {
    using frontier_entry_t = std::atomic<uint16_t>;

public:
    static constexpr uint16_t UNVISITED = UINT16_MAX;
    static constexpr uint16_t INITIAL_VISITED = 0;

    PathLengths(const common::table_id_map_t<common::offset_t>& nodeMaxOffsetMap,
        storage::MemoryManager* mm);
    PathLengths(const PathLengths& other) = delete;
    PathLengths(const PathLengths&& other) = delete;
    ~PathLengths();

    const common::table_id_map_t<common::offset_t>& getNodeMaxOffsetMap() const {
        return nodeMaxOffsetMap;
    }

    uint16_t getMaskValueFromCurFrontier(common::offset_t offset) {
        return curFrontier[offset].load(std::memory_order_relaxed);
    }
    uint16_t getMaskValueFromNextFrontier(common::offset_t offset) {
        return nextFrontier[offset].load(std::memory_order_relaxed);
    }

    bool isActive(common::offset_t offset) { return curFrontier[offset] == curIter - 1; }

    void setActive(std::span<const common::nodeID_t> nodeIDs) {
        for (const auto nodeID : nodeIDs) {
            setActive(nodeID.offset);
        }
    }
    void setActive(common::nodeID_t nodeID) { setActive(nodeID.offset); }
    void setActive(common::offset_t offset) {
        nextFrontier[offset].store(curIter, std::memory_order_relaxed);
    }
    void setCurFrontierValue(common::offset_t offset, uint16_t val) {
        curFrontier[offset].store(val, std::memory_order_relaxed);
    }

    void incrementCurIter() { curIter++; }
    void resetCurIter() { curIter = 0; }

    void pinTableID(common::table_id_t tableID) { pinCurFrontierTableID(tableID); }
    void pinCurFrontierTableID(common::table_id_t tableID) { curFrontier = getMaskData(tableID); }
    void pinNextFrontierTableID(common::table_id_t tableID) { nextFrontier = getMaskData(tableID); }

    // Init frontier to UNVISITED
    static std::shared_ptr<PathLengths> getUnvisitedFrontier(processor::ExecutionContext* context,
        graph::Graph* graph);
    // Init frontier to 0 (first iteration)
    static std::shared_ptr<PathLengths> getVisitedFrontier(processor::ExecutionContext* context,
        graph::Graph* graph);
    // Init frontier to 0 according to mask
    static std::shared_ptr<PathLengths> getVisitedFrontier(processor::ExecutionContext* context,
        graph::Graph* graph, common::NodeOffsetMaskMap* maskMap);

private:
    frontier_entry_t* getMaskData(common::table_id_t tableID);

private:
    common::table_id_map_t<common::offset_t> nodeMaxOffsetMap;
    common::table_id_map_t<std::unique_ptr<storage::MemoryBuffer>> masks;
    // See FrontierPair::curIter. We keep a copy of curIter here because PathLengths stores
    // iteration numbers for vertices and uses them to identify which vertex is in the frontier.
    uint16_t curIter = 0;
    frontier_entry_t* curFrontier = nullptr;
    frontier_entry_t* nextFrontier = nullptr;
};

class PathLengthsInitVertexCompute : public VertexCompute {
public:
    PathLengthsInitVertexCompute(PathLengths& pathLengths, uint16_t val)
        : pathLengths{pathLengths}, val{val} {}

    bool beginOnTable(common::table_id_t tableID) override;

    void vertexCompute(common::offset_t startOffset, common::offset_t endOffset,
        common::table_id_t) override;

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<PathLengthsInitVertexCompute>(pathLengths, val);
    }

private:
    PathLengths& pathLengths;
    uint16_t val;
};

/**
 * Base class for maintaining a current and a next GDSFrontier of nodes for GDS algorithms. At any
 * point in time, maintains the current iteration curIter the algorithm is in and the number of
 * active nodes that have been set for the next iteration. This information can be used
 * to determine if the algorithm has converged or not.
 *
 * All functions supported in this base interface are thread-safe.
 */
class KUZU_API FrontierPair {
public:
    FrontierPair(std::shared_ptr<PathLengths> curFrontier,
        std::shared_ptr<PathLengths> nextFrontier);

    virtual ~FrontierPair() = default;

    void setActiveNodesForNextIter() { hasActiveNodesForNextIter_.store(true); }

    void beginNewIteration();

    // Initialize for recursive computation which always starts from a single source.
    virtual void initSource(common::nodeID_t source);
    // Initialize for gds computation which usually starts from a large number of nodes;
    void initGDS();
    void initState();

    virtual void beginFrontierComputeBetweenTables(common::table_id_t curTableID,
        common::table_id_t nextTableID);

    void pinCurrFrontier(common::table_id_t tableID) {
        curSparseFrontier->pinTableID(tableID);
        curDenseFrontier->pinCurFrontierTableID(tableID);
    }
    void pinNextFrontier(common::table_id_t tableID) {
        nextSparseFrontier->pinTableID(tableID);
        nextDenseFrontier->pinNextFrontierTableID(tableID);
    }

    uint16_t getCurrentIter() { return curIter.load(std::memory_order_relaxed); }

    PathLengths& getCurDenseFrontier() const { return *curDenseFrontier; }
    SparseFrontier& getCurSparseFrontier() const { return *curSparseFrontier; }
    PathLengths& getNextDenseFrontier() const { return *nextDenseFrontier; }
    SparseFrontier& getNextSparseFrontier() const { return *nextSparseFrontier; }
    SparseFrontier& getVertexComputeCandidates() const { return *vertexComputeCandidates; }

    bool continueNextIter(uint16_t maxIter);

    void addNodeToNextDenseFrontier(common::nodeID_t nodeID);
    void addNodesToNextDenseFrontier(const std::vector<common::nodeID_t>& nodeIDs);

    void mergeLocalFrontier(const SparseFrontier& localFrontier);

    bool isCurFrontierSparse();

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }

protected:
    virtual void beginNewIterationInternalNoLock() {}

protected:
    std::mutex mtx;
    // curIter is the iteration number of the algorithm and starts from 0.
    std::atomic<uint16_t> curIter;
    std::atomic<bool> hasActiveNodesForNextIter_;

    // Dense frontiers are always updated.
    std::shared_ptr<PathLengths> curDenseFrontier;
    std::shared_ptr<PathLengths> nextDenseFrontier;
    // Sparse frontiers
    std::shared_ptr<SparseFrontier> curSparseFrontier;
    std::shared_ptr<SparseFrontier> nextSparseFrontier;
    std::shared_ptr<SparseFrontier> vertexComputeCandidates;
};

class SinglePathLengthsFrontierPair : public FrontierPair {
public:
    explicit SinglePathLengthsFrontierPair(std::shared_ptr<PathLengths> pathLengths)
        : FrontierPair(pathLengths /* curFrontier */, pathLengths /* nextFrontier */),
          pathLengths{std::move(pathLengths)} {}

    PathLengths* getPathLengths() const { return pathLengths.get(); }

    void beginNewIterationInternalNoLock() override { pathLengths->incrementCurIter(); }

private:
    std::shared_ptr<PathLengths> pathLengths;
};

class KUZU_API DoublePathLengthsFrontierPair : public FrontierPair {
public:
    DoublePathLengthsFrontierPair(std::shared_ptr<PathLengths> curFrontier,
        std::shared_ptr<PathLengths> nextFrontier)
        : FrontierPair{curFrontier, nextFrontier} {}

    void beginNewIterationInternalNoLock() override;
};

class SPEdgeCompute : public EdgeCompute {
public:
    explicit SPEdgeCompute(SinglePathLengthsFrontierPair* frontierPair)
        : frontierPair{frontierPair}, numNodesReached{0} {}

    void resetSingleThreadState() override { numNodesReached = 0; }

    bool terminate(common::NodeOffsetMaskMap& maskMap) override;

protected:
    SinglePathLengthsFrontierPair* frontierPair;
    // States that should be only modified with single thread
    common::offset_t numNodesReached;
};

} // namespace function
} // namespace kuzu
