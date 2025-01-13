#pragma once

#include <utility>

#include "common/enums/extend_direction.h"
#include "common/task_system/task.h"
#include "function/gds/gds_frontier.h"
#include "graph/graph.h"

namespace kuzu {
namespace function {

struct FrontierTaskInfo {
    catalog::TableCatalogEntry* boundEntry = nullptr;
    catalog::TableCatalogEntry* nbrEntry = nullptr;
    catalog::TableCatalogEntry* relEntry = nullptr;
    graph::Graph* graph;
    common::ExtendDirection direction;
    EdgeCompute& edgeCompute;
    std::string propertyToScan;

    FrontierTaskInfo(catalog::TableCatalogEntry* boundEntry, catalog::TableCatalogEntry* nbrEntry,
        catalog::TableCatalogEntry* relEntry, graph::Graph* graph,
        common::ExtendDirection direction, EdgeCompute& edgeCompute, std::string propertyToScan)
        : boundEntry{boundEntry}, nbrEntry{nbrEntry}, relEntry{relEntry}, graph{graph},
          direction{direction}, edgeCompute{edgeCompute},
          propertyToScan{std::move(propertyToScan)} {}
    FrontierTaskInfo(const FrontierTaskInfo& other)
        : boundEntry{other.boundEntry}, nbrEntry{other.nbrEntry}, relEntry{other.relEntry},
          graph{other.graph}, direction{other.direction}, edgeCompute{other.edgeCompute},
          propertyToScan{other.propertyToScan} {}
};

struct FrontierTaskSharedState {
    FrontierMorselDispatcher morselDispatcher;
    FrontierPair& frontierPair;

    FrontierTaskSharedState(uint64_t maxNumThreads, FrontierPair& frontierPair)
        : morselDispatcher{maxNumThreads}, frontierPair{frontierPair} {}
    DELETE_COPY_AND_MOVE(FrontierTaskSharedState);
};

class FrontierTask : public common::Task {
public:
    FrontierTask(uint64_t maxNumThreads, const FrontierTaskInfo& info,
        std::shared_ptr<FrontierTaskSharedState> sharedState)
        : common::Task{maxNumThreads}, info{info}, sharedState{std::move(sharedState)} {}

    void run() override;

    void runSparse();

private:
    FrontierTaskInfo info;
    std::shared_ptr<FrontierTaskSharedState> sharedState;
};

struct VertexComputeTaskSharedState {
    FrontierMorselDispatcher morselDispatcher;

    explicit VertexComputeTaskSharedState(uint64_t maxNumThreads)
        : morselDispatcher{maxNumThreads} {}
};

struct VertexComputeTaskInfo {
    VertexCompute& vc;
    graph::Graph* graph;
    catalog::TableCatalogEntry* tableEntry;
    std::vector<std::string> propertiesToScan;

    VertexComputeTaskInfo(VertexCompute& vc, graph::Graph* graph,
        catalog::TableCatalogEntry* tableEntry, std::vector<std::string> propertiesToScan)
        : vc{vc}, graph{graph}, tableEntry{tableEntry},
          propertiesToScan{std::move(propertiesToScan)} {}
    VertexComputeTaskInfo(const VertexComputeTaskInfo& other)
        : vc{other.vc}, graph{other.graph}, tableEntry{other.tableEntry},
          propertiesToScan{other.propertiesToScan} {}

    bool hasPropertiesToScan() const { return !propertiesToScan.empty(); }
};

class VertexComputeTask : public common::Task {
public:
    VertexComputeTask(uint64_t maxNumThreads, const VertexComputeTaskInfo& info,
        std::shared_ptr<VertexComputeTaskSharedState> sharedState)
        : common::Task{maxNumThreads}, info{info}, sharedState{std::move(sharedState)} {};

    VertexComputeTaskSharedState* getSharedState() const { return sharedState.get(); }

    void run() override;

private:
    VertexComputeTaskInfo info;
    std::shared_ptr<VertexComputeTaskSharedState> sharedState;
};

} // namespace function
} // namespace kuzu
