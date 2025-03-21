#pragma once

#include <atomic>
#include <mutex>

#include "compute.h"
#include "density_state.h"
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

using iteration_t = uint16_t;

class KUZU_API SparseFrontier {
    friend class SPFrontierPair;

public:
    SparseFrontier() = default;

    void pinTableID(common::table_id_t tableID);
    const std::unordered_set<common::offset_t>& getOffsetSet() const { return *curOffsetSet; }

    void addNode(common::nodeID_t nodeID) {
        KU_ASSERT(curOffsetSet);
        curOffsetSet->insert(nodeID.offset);
    }
    void addNodes(const std::vector<common::nodeID_t>& nodeIDs) {
        KU_ASSERT(curOffsetSet);
        for (auto nodeID : nodeIDs) {
            curOffsetSet->insert(nodeID.offset);
        }
    }
    uint64_t size() const {
        auto result = 0u;
        for (auto [_, set] : tableIDToOffsetMap) {
            result += set.size();
        }
        return result;
    }

private:
    common::table_id_t pinnedTableID = common::INVALID_TABLE_ID;
    std::unordered_map<common::table_id_t, std::unordered_set<common::offset_t>> tableIDToOffsetMap;
    std::unordered_set<common::offset_t>* curOffsetSet = nullptr;
};

class KUZU_API PathLengths {
    friend class SPFrontierPair;
public:
    static constexpr iteration_t UNVISITED = UINT16_MAX;
    static constexpr iteration_t INITIAL_VISITED = 0;

    explicit PathLengths(const common::table_id_map_t<common::offset_t>& nodeMaxOffsetMap)
        : nodeMaxOffsetMap{nodeMaxOffsetMap} {}
    PathLengths(const PathLengths& other) = delete;
    PathLengths(const PathLengths&& other) = delete;
    ~PathLengths() = default;

    void init(processor::ExecutionContext* context, graph::Graph* graph, iteration_t val);

    const common::table_id_map_t<common::offset_t>& getNodeMaxOffsetMap() const {
        return nodeMaxOffsetMap;
    }

    void pinTableID(common::table_id_t tableID) {
        data = getMaskData(tableID);
    }

    iteration_t getIteration(common::offset_t offset) const {
        return data[offset].load(std::memory_order_relaxed);
    }

    void setIteration(common::offset_t offset, iteration_t value) {
        KU_ASSERT(data);
        data[offset].store(value, std::memory_order_relaxed);
    }

    bool isUnvisited(common::offset_t offset) const {
        KU_ASSERT(data);
        return data[offset].load(std::memory_order_relaxed) == UNVISITED;
    }

    static std::shared_ptr<PathLengths> getUninitializedFrontier(processor::ExecutionContext* context,
        graph::Graph* graph);
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
    // TODO rename me
    std::atomic<iteration_t>* getMaskData(common::table_id_t tableID);

private:
    common::table_id_map_t<common::offset_t> nodeMaxOffsetMap;
    // TODO: change me
    common::table_id_map_t<std::unique_ptr<storage::MemoryBuffer>> buffer;
    std::atomic<iteration_t>* data = nullptr;
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

class KUZU_API FrontierPair {
public:
    FrontierPair() { initState(); }
    virtual ~FrontierPair() = default;

    // Get current iteration number
    uint16_t getCurrentIter() { return curIter.load(std::memory_order_relaxed); }
    void setActiveNodesForNextIter() { hasActiveNodesForNextIter_.store(true); }
    bool continueNextIter(uint16_t maxIter) {
        return hasActiveNodesForNextIter_.load(std::memory_order_relaxed) && getCurrentIter() < maxIter;
    }
    // TODO: remove me
    void initState();

    void beginNewIteration();
    void beginFrontierComputeBetweenTables(common::table_id_t curTableID,
        common::table_id_t nextTableID) {
        pinCurrentFrontier(curTableID);
        pinNextFrontier(nextTableID);
    }
    virtual void pinCurrentFrontier(common::table_id_t tableID) = 0;
    virtual void pinNextFrontier(common::table_id_t tableID) = 0;

    // Add nodes to next frontier
    virtual void addNodeToNextSparseFrontier(common::nodeID_t nodeID) = 0;
    virtual void addNodesToNextSpareFrontier(const std::vector<common::nodeID_t>& nodeIDs) = 0;
    virtual void addNodesToNextDenseFrontier(const std::unordered_set<common::offset_t>& offsets) = 0;
    virtual void addNodesToNextDenseFrontier(const std::vector<common::nodeID_t>& nodeIDs) = 0;

    // If given offset is active on current frontier.
    virtual bool isActive(common::offset_t offset) = 0;
    // Get all
    virtual const std::unordered_set<common::offset_t>& getActiveNodes() = 0;

    virtual FrontierState getState() const = 0;
    virtual bool needSwitchToDense() const = 0;
    virtual void switchToDense(processor::ExecutionContext* context, graph::Graph* graph) = 0;

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
};

class SPFrontierPair : public FrontierPair {
public:
    explicit SPFrontierPair(std::shared_ptr<PathLengths> pathLengths)
        : pathLengths {std::move(pathLengths)}, state{FrontierState::SPARSE} {
        curSparseFrontier = std::make_unique<SparseFrontier>();
        nextSparseFrontier = std::make_unique<SparseFrontier>();
        // vertexComputeCandidate = std::make_unique<SparseFrontier>();
    }

    void pinCurrentFrontier(common::table_id_t tableID) override;
    void pinNextFrontier(common::table_id_t tableID) override;

    void beginNewIterationInternalNoLock() override;

    void addNodeToNextSparseFrontier(common::nodeID_t nodeID) override;
    void addNodesToNextSpareFrontier(const std::vector<common::nodeID_t> &nodeIDs) override;
    void addNodesToNextDenseFrontier(const std::unordered_set<common::offset_t> &offsets) override;
    void addNodesToNextDenseFrontier(const std::vector<common::nodeID_t> &nodeIDs) override;

    iteration_t getNextDenseFrontierValue(common::offset_t offset);

    common::offset_t getNumActiveNodesInCurrentFrontier(common::NodeOffsetMaskMap &mask);

    bool isActive(common::offset_t offset) override;
    const std::unordered_set<common::offset_t>& getActiveNodes() override {
        KU_ASSERT(curSparseFrontier);
        return curSparseFrontier->getOffsetSet();
    }

    GDSDensityState getState() const override {
        return state;
    }
    bool needSwitchToDense() const override {
        return state == GDSDensityState::SPARSE && nextSparseFrontier->size() > 1000;
    }
    void switchToDense(processor::ExecutionContext* context, graph::Graph* graph) override;

private:
    GDSDensityState state;
    std::shared_ptr<PathLengths> pathLengths;
    std::atomic<iteration_t>* curDenseFrontier = nullptr;
    std::atomic<iteration_t>* nextDenseFrontier = nullptr;
    std::unique_ptr<SparseFrontier> curSparseFrontier = nullptr;
    std::unique_ptr<SparseFrontier> nextSparseFrontier = nullptr;
    // TODO this is not clear.
    // std::unique_ptr<SparseFrontier> vertexComputeCandidate = nullptr;
};

class KUZU_API DoublePathLengthsFrontierPair : public FrontierPair {
public:
    DoublePathLengthsFrontierPair(std::shared_ptr<PathLengths> curDenseFrontier,
        std::shared_ptr<PathLengths> nextDenseFrontier)
        : curDenseFrontier{std::move(curDenseFrontier)}, nextDenseFrontier {std::move(nextDenseFrontier)} {}

    void pinCurrentFrontier(common::table_id_t tableID) override;
    void pinNextFrontier(common::table_id_t tableID) override;

    void beginNewIterationInternalNoLock() override;

    void addNodeToNextSparseFrontier(common::nodeID_t nodeID) override {
        KU_UNREACHABLE;
    }
    void addNodesToNextSpareFrontier(const std::vector<common::nodeID_t> &nodeIDs) override {
        KU_UNREACHABLE;
    }
    void addNodesToNextDenseFrontier(const std::vector<common::nodeID_t> &nodeIDs) override;

    void setCurFrontierValue(common::offset_t offset, iteration_t value);
    void setNextFrontierValue(common::offset_t offset, iteration_t value);

    bool isActive(common::offset_t offset) override;
    const std::unordered_set<common::offset_t> &getActiveNodes() override {
        KU_UNREACHABLE;
    }

    bool isSparse() override {
        return false;
    }

private:
    std::shared_ptr<PathLengths> curDenseFrontier;
    std::shared_ptr<PathLengths> nextDenseFrontier;
};

// class RecursiveJoinFrontierPair : public DoublePathLengthsFrontierPair {
//
// private:
//     std::unique_ptr<SparseFrontier> curSparseFrontier = nullptr;
//     std::unique_ptr<SparseFrontier> nextSparseFrontier = nullptr;
//     std::shared_ptr<PathLengths> curDenseFrontier;
//     std::shared_ptr<PathLengths> nextDenseFrontier;
// };

class SPEdgeCompute : public EdgeCompute {
public:
    explicit SPEdgeCompute(SPFrontierPair* frontierPair)
        : frontierPair{frontierPair}, numNodesReached{0} {}

    void resetSingleThreadState() override { numNodesReached = 0; }

    bool terminate(common::NodeOffsetMaskMap& maskMap) override;

protected:
    SPFrontierPair* frontierPair;
    // States that should be only modified with single thread
    common::offset_t numNodesReached;
};

} // namespace function
} // namespace kuzu
