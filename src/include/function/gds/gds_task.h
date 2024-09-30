#pragma once

#include "common/task_system/task.h"
#include "function/gds/gds_frontier.h"
#include "graph/graph.h"

namespace kuzu {
namespace function {

class FrontierTaskSharedState {
public:
    FrontierTaskSharedState(FrontierPair& frontierPair, graph::Graph* graph, EdgeCompute& ec,
        common::table_id_t relTableIDToScan)
        : frontierPair{frontierPair}, graph{graph}, ec{ec}, relTableIDToScan{relTableIDToScan} {};

public:
    FrontierPair& frontierPair;
    graph::Graph* graph;
    EdgeCompute& ec;
    common::table_id_t relTableIDToScan;
};

class FrontierTask : public common::Task {
public:
    FrontierTask(uint64_t maxNumThreads, std::shared_ptr<FrontierTaskSharedState> sharedState)
        : common::Task{maxNumThreads}, sharedState{std::move(sharedState)} {}

    void run() override;

private:
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
