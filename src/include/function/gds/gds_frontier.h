#pragma once

#include <atomic>
#include <mutex>

#include "compute.h"
#include "density_state.h"
#include "gds_object_manager.h"
#include <span>

namespace kuzu {
namespace processor {
struct ExecutionContext;
}
namespace function {

using iteration_t = uint16_t;
static constexpr iteration_t FRONTIER_UNVISITED = UINT16_MAX;
static constexpr iteration_t FRONTIER_INITIAL_VISITED = 0;

class Frontier {
public:
    virtual ~Frontier() =  default;

    virtual void pinTableID(common::table_id_t tableID) = 0;

    virtual void addNode(common::nodeID_t nodeID, iteration_t iter) = 0;
    virtual void addNodes(const std::vector<common::nodeID_t>& nodeIDs, iteration_t iter) = 0;

    virtual iteration_t getIteration(common::offset_t offset) const;
};

class KUZU_API SparseFrontier : public Frontier {
    friend class SPFrontierPair;
    friend class VarLengthFrontierPair;

public:
    void pinTableID(common::table_id_t tableID) override;

    void addNode(common::nodeID_t nodeID, iteration_t iter) override;
    void addNodes(const std::vector<common::nodeID_t>& nodeIDs, iteration_t iter) override;

    iteration_t getIteration(common::offset_t offset) const override;

    uint64_t size() const { return sparseObjects.size(); }

private:
    GDSSpareObjectManager<iteration_t> sparseObjects;
    std::unordered_map<common::offset_t, iteration_t>* curData = nullptr;
};

class KUZU_API DenseFrontier : public Frontier {
    friend class DenseFrontierReference;
    friend class SPFrontierPair;
    friend class VarLengthFrontierPair;
    friend class PathLengthsInitVertexCompute;

public:
    explicit DenseFrontier(const common::table_id_map_t<common::offset_t>& nodeMaxOffsetMap)
        : nodeMaxOffsetMap{nodeMaxOffsetMap} {}
    DenseFrontier(const DenseFrontier& other) = delete;
    DenseFrontier(const DenseFrontier&& other) = delete;

    // Allocate memory and initialize.
    void init(processor::ExecutionContext* context, graph::Graph* graph, iteration_t val);

    void pinTableID(common::table_id_t tableID) override;

    void addNode(common::nodeID_t nodeID, iteration_t iter) override;
    void addNodes(const std::vector<common::nodeID_t>& nodeIDs, iteration_t iter) override;

    iteration_t getIteration(common::offset_t offset) const override;

    // Get frontier without initialization.
    static std::shared_ptr<DenseFrontier> getUninitializedFrontier(processor::ExecutionContext* context,
        graph::Graph* graph);
    // Get frontier initialized to UNVISITED.
    static std::shared_ptr<DenseFrontier> getUnvisitedFrontier(processor::ExecutionContext* context,
        graph::Graph* graph);
    // Get frontier initialized to INITIAL_VISITED.
    static std::shared_ptr<DenseFrontier> getVisitedFrontier(processor::ExecutionContext* context,
        graph::Graph* graph);
    // Init frontier to 0 according to mask
    static std::shared_ptr<DenseFrontier> getVisitedFrontier(processor::ExecutionContext* context,
        graph::Graph* graph, common::NodeOffsetMaskMap* maskMap);

private:
    common::table_id_map_t<common::offset_t> nodeMaxOffsetMap;
    GDSDenseObjectManager<std::atomic<iteration_t>> denseObjects;
    std::atomic<iteration_t>* curData = nullptr;
};

class DenseFrontierReference : public Frontier {
    friend class SPFrontierPair;
public:
    DenseFrontierReference(const DenseFrontier& denseFrontier) : denseObjects{denseFrontier.denseObjects} {}

    void pinTableID(common::table_id_t tableID) override;

    void addNode(common::nodeID_t nodeID, iteration_t iter) override;
    void addNodes(const std::vector<common::nodeID_t> &nodeIDs, iteration_t iter) override;

    iteration_t getIteration(common::offset_t offset) const override;

private:
    const GDSDenseObjectManager<std::atomic<iteration_t>>& denseObjects;
    std::atomic<iteration_t>* curData = nullptr;
};

class PathLengthsInitVertexCompute : public VertexCompute {
public:
    PathLengthsInitVertexCompute(DenseFrontier& frontier, iteration_t val)
        : frontier{frontier}, val{val} {}

    bool beginOnTable(common::table_id_t tableID) override;

    void vertexCompute(common::offset_t startOffset, common::offset_t endOffset,
        common::table_id_t) override;

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<PathLengthsInitVertexCompute>(frontier, val);
    }

private:
    DenseFrontier& frontier;
    iteration_t val;
};

class KUZU_API FrontierPair {
public:
    FrontierPair() { initState(); }
    virtual ~FrontierPair() = default;

    iteration_t getCurrentIter() { return curIter; }

    void setActiveNodesForNextIter() { hasActiveNodesForNextIter_.store(true); }

    bool continueNextIter(uint16_t maxIter) {
        return hasActiveNodesForNextIter_.load(std::memory_order_relaxed) && getCurrentIter() < maxIter;
    }

    // TODO: remove me
    void initState() {
        hasActiveNodesForNextIter_.store(false);
        curIter = 0;
    }

    void beginNewIteration();
    void pinCurrentFrontier(common::table_id_t tableID);
    void pinNextFrontier(common::table_id_t tableID);
    void beginFrontierComputeBetweenTables(common::table_id_t curTableID,
        common::table_id_t nextTableID);

    void addNodeToNextFrontier(common::nodeID_t nodeID);
    void addNodesToNextFrontier(const std::vector<common::nodeID_t>& nodeIDs);

    iteration_t getNextFrontierValue(common::offset_t offset);

    bool isActiveOnCurrentFrontier(common::offset_t offset);

    virtual GDSDensityState getState() const = 0;
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
    iteration_t curIter;
    std::atomic<bool> hasActiveNodesForNextIter_;
    Frontier* currentFrontier = nullptr;
    Frontier* nextFrontier = nullptr;
};

class SPFrontierPair : public FrontierPair {
public:
    explicit SPFrontierPair(std::shared_ptr<DenseFrontier> denseFrontier)
        : state{GDSDensityState::SPARSE}, denseFrontier {std::move(denseFrontier)} {
        curSparseFrontier = std::make_unique<SparseFrontier>();
        nextSparseFrontier = std::make_unique<SparseFrontier>();
        curDenseFrontier = std::make_unique<DenseFrontierReference>(*this->denseFrontier);
    }

    DenseFrontier& getDenseFrontier() const { return *denseFrontier; }

    void beginNewIterationInternalNoLock() override;

    common::offset_t getNumActiveNodesInCurrentFrontier(common::NodeOffsetMaskMap &mask);

    GDSDensityState getState() const override { return state; }
    bool needSwitchToDense() const override {
        return state == GDSDensityState::SPARSE && nextSparseFrontier->size() > 1000;
    }
    void switchToDense(processor::ExecutionContext* context, graph::Graph* graph) override;

private:
    GDSDensityState state;
    std::shared_ptr<DenseFrontier> denseFrontier;
    std::unique_ptr<DenseFrontierReference> curDenseFrontier = nullptr;
    std::unique_ptr<DenseFrontierReference> nextDenseFrontier = nullptr;
    std::unique_ptr<SparseFrontier> curSparseFrontier = nullptr;
    std::unique_ptr<SparseFrontier> nextSparseFrontier = nullptr;
};

class VarLengthFrontierPair : public FrontierPair {
public:
    VarLengthFrontierPair(std::shared_ptr<DenseFrontier> curDenseFrontier, std::shared_ptr<DenseFrontier> nextDenseFrontier)
        : state{GDSDensityState::SPARSE}, curDenseFrontier{std::move(curDenseFrontier)}, nextDenseFrontier{std::move(nextDenseFrontier)} {
        curSparseFrontier = std::make_unique<SparseFrontier>();
        nextSparseFrontier = std::make_unique<SparseFrontier>();
    }

    void beginNewIterationInternalNoLock() override;

    GDSDensityState getState() const override { return state; }
    bool needSwitchToDense() const override {
        return state == GDSDensityState::SPARSE && nextSparseFrontier->size() > 1000;
    }
    void switchToDense(processor::ExecutionContext *context, graph::Graph *graph) override;

private:
    GDSDensityState state;
    std::shared_ptr<DenseFrontier> curDenseFrontier;
    std::shared_ptr<DenseFrontier> nextDenseFrontier;
    std::unique_ptr<SparseFrontier> curSparseFrontier = nullptr;
    std::unique_ptr<SparseFrontier> nextSparseFrontier = nullptr;
};

class KUZU_API DenseFrontierPair : public FrontierPair {
public:
    DenseFrontierPair(std::shared_ptr<DenseFrontier> curDenseFrontier,
        std::shared_ptr<DenseFrontier> nextDenseFrontier)
        : curDenseFrontier{std::move(curDenseFrontier)}, nextDenseFrontier {std::move(nextDenseFrontier)} {}

    void beginNewIterationInternalNoLock() override;

    GDSDensityState getState() const override { return GDSDensityState::DENSE; }
    bool needSwitchToDense() const override { return false; }
    void switchToDense(processor::ExecutionContext *context, graph::Graph *graph) override {
        // Do nothing.
    }

private:
    std::shared_ptr<DenseFrontier> curDenseFrontier;
    std::shared_ptr<DenseFrontier> nextDenseFrontier;
};

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
