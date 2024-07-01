#pragma once

#include "common/task_system/task.h"
#include "function/gds/gds_frontier.h"

namespace kuzu {
namespace function {

class FrontierTaskSharedState {
public:
    FrontierTaskSharedState(Frontiers& frontiers, graph::Graph* graph, FrontierCompute& fc,
        table_id_t relTableIDToScan)
        : frontiers{frontiers}, graph{graph}, fc{fc}, relTableIDToScan{relTableIDToScan} {};

public:
    Frontiers& frontiers;
    graph::Graph* graph;
    FrontierCompute& fc;
    table_id_t relTableIDToScan;
};

class GDSTask : public common::Task {

public:
    GDSTask(uint64_t maxNumThreads, std::shared_ptr<FrontierTaskSharedState> sharedState)
        : common::Task{maxNumThreads}, sharedState{std::move(sharedState)} {}

    void run() override;

private:
    std::shared_ptr<FrontierTaskSharedState> sharedState;
};
} // namespace function
} // namespace kuzu
