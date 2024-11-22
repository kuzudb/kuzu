#pragma once

#include "common/enums/extend_direction.h"
#include "common/task_system/task.h"
#include "function/gds/gds_frontier.h"
#include "graph/graph.h"

namespace kuzu {
namespace function {

struct FrontierTaskInfo {
    common::table_id_t relTableIDToScan;
    graph::Graph* graph;
    common::ExtendDirection direction;
    EdgeCompute& edgeCompute;
    std::optional<common::idx_t> edgePropertyIdx;

    FrontierTaskInfo(common::table_id_t tableID, graph::Graph* graph,
        common::ExtendDirection direction, EdgeCompute& edgeCompute,
        std::optional<common::idx_t> edgePropertyIdx)
        : relTableIDToScan{tableID}, graph{graph}, direction{direction}, edgeCompute{edgeCompute},
          edgePropertyIdx{edgePropertyIdx} {}
    FrontierTaskInfo(const FrontierTaskInfo& other)
        : relTableIDToScan{other.relTableIDToScan}, graph{other.graph}, direction{other.direction},
          edgeCompute{other.edgeCompute}, edgePropertyIdx{other.edgePropertyIdx} {}
};

struct FrontierTaskSharedState {
    FrontierPair& frontierPair;

    explicit FrontierTaskSharedState(FrontierPair& frontierPair) : frontierPair{frontierPair} {}
    DELETE_COPY_AND_MOVE(FrontierTaskSharedState);
};

class FrontierTask : public common::Task {
public:
    FrontierTask(uint64_t maxNumThreads, const FrontierTaskInfo& info,
        std::shared_ptr<FrontierTaskSharedState> sharedState)
        : common::Task{maxNumThreads}, info{info}, sharedState{std::move(sharedState)} {}

    void run() override;

private:
    FrontierTaskInfo info;
    std::shared_ptr<FrontierTaskSharedState> sharedState;
};

struct VertexComputeTaskSharedState {
    FrontierMorselDispatcher morselDispatcher;
    graph::Graph* graph;

    explicit VertexComputeTaskSharedState(uint64_t maxThreadsForExecution, graph::Graph* graph)
        : morselDispatcher{maxThreadsForExecution}, graph{graph} {}
};

struct VertexComputeTaskInfo {
    VertexCompute& vc;
    std::vector<std::string> propertiesToScan;

    explicit VertexComputeTaskInfo(VertexCompute& vc, std::vector<std::string> propertiesToScan)
        : vc{vc}, propertiesToScan{std::move(propertiesToScan)} {}
    VertexComputeTaskInfo(const VertexComputeTaskInfo& other)
        : vc{other.vc}, propertiesToScan{other.propertiesToScan} {}
};

class VertexComputeTask : public common::Task {
public:
    VertexComputeTask(uint64_t maxNumThreads, const VertexComputeTaskInfo& info,
        std::shared_ptr<VertexComputeTaskSharedState> sharedState)
        : common::Task{maxNumThreads}, info{info}, sharedState{std::move(sharedState)} {};

    void run() override;

private:
    VertexComputeTaskInfo info;
    std::shared_ptr<VertexComputeTaskSharedState> sharedState;
};
} // namespace function
} // namespace kuzu
