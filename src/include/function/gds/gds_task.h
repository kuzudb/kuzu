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
    std::optional<common::idx_t> edgePropertyIndex;

    FrontierTaskInfo(common::table_id_t tableID, graph::Graph* graph,
        common::ExtendDirection direction, EdgeCompute& edgeCompute,
        std::optional<common::idx_t> edgePropertyIndex)
        : relTableIDToScan{tableID}, graph{graph}, direction{direction}, edgeCompute{edgeCompute},
          edgePropertyIndex{std::move(edgePropertyIndex)} {}
    FrontierTaskInfo(const FrontierTaskInfo& other)
        : relTableIDToScan{other.relTableIDToScan}, graph{other.graph}, direction{other.direction},
          edgeCompute{other.edgeCompute}, edgePropertyIndex{other.edgePropertyIndex} {}
};

struct FrontierTaskSharedState {
    FrontierPair& frontierPair;

    explicit FrontierTaskSharedState(FrontierPair& frontierPair) : frontierPair{frontierPair} {}
    DELETE_COPY_AND_MOVE(FrontierTaskSharedState);
};

class KUZU_API FrontierTask : public common::Task {
public:
    FrontierTask(uint64_t maxNumThreads, const FrontierTaskInfo& info,
        std::shared_ptr<FrontierTaskSharedState> sharedState)
        : common::Task{maxNumThreads}, info{info}, sharedState{std::move(sharedState)} {}

    void run() override;

private:
    FrontierTaskInfo info;
    std::shared_ptr<FrontierTaskSharedState> sharedState;
};

struct KUZU_API VertexComputeTaskSharedState {
    FrontierMorselDispatcher morselDispatcher;
    std::vector<std::string> propertiesToScan;
    graph::Graph* graph;

    VertexComputeTaskSharedState(uint64_t maxThreadsForExecution,
        std::vector<std::string> propertiesToScan, graph::Graph* graph)
        : morselDispatcher{maxThreadsForExecution}, propertiesToScan{std::move(propertiesToScan)},
          graph{graph} {}
};

struct VertexComputeTaskInfo {
    VertexCompute& vc;

    explicit VertexComputeTaskInfo(VertexCompute& vc) : vc{vc} {}
    VertexComputeTaskInfo(const VertexComputeTaskInfo& other) : vc{other.vc} {}
};

class KUZU_API VertexComputeTask : public common::Task {
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
