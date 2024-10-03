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

    FrontierTaskInfo(common::table_id_t tableID, graph::Graph* graph,
        common::ExtendDirection direction, EdgeCompute& edgeCompute)
        : relTableIDToScan{tableID}, graph{graph}, direction{direction}, edgeCompute{edgeCompute} {}
    FrontierTaskInfo(const FrontierTaskInfo& other)
        : relTableIDToScan{other.relTableIDToScan}, graph{other.graph}, direction{other.direction},
          edgeCompute{other.edgeCompute} {}
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

class VertexComputeTaskSharedState {
public:
    VertexComputeTaskSharedState(graph::Graph* graph, VertexCompute& vc,
        uint64_t maxThreadsForExecution);

public:
    graph::Graph* graph;
    VertexCompute& vc;
    std::unique_ptr<FrontierMorselDispatcher> morselDispatcher;
};

class VertexComputeTask : public common::Task {
public:
    VertexComputeTask(uint64_t maxNumThreads,
        std::shared_ptr<VertexComputeTaskSharedState> sharedState)
        : common::Task{maxNumThreads}, sharedState{std::move(sharedState)} {};

    void run() override;

private:
    std::shared_ptr<VertexComputeTaskSharedState> sharedState;
};
} // namespace function
} // namespace kuzu
