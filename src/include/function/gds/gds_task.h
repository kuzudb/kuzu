#pragma once

#include "common/task_system/task.h"
#include "function/gds/gds_frontier.h"
#include "function/table_functions.h"

namespace kuzu {
namespace function {

class FrontierTaskSharedState {
public:
    static constexpr const int64_t MORSEL_SIZE = 256;
    FrontierTaskSharedState(Frontiers& frontiers, graph::Graph* graph, FrontierUpdateFn& vu, table_id_t relTableIDToScan)
        : frontiers{frontiers}, graph{graph}, vu{vu}, relTableIDToScan{relTableIDToScan} {};

public:
    Frontiers& frontiers;
    graph::Graph* graph;
    FrontierUpdateFn& vu;
    table_id_t relTableIDToScan;
};

class FrontierTask : public common::Task {

public:
    FrontierTask(uint64_t maxNumThreads, std::shared_ptr<FrontierTaskSharedState> sharedState)
        : common::Task{maxNumThreads}, sharedState{std::move(sharedState)} {}

    void run() override;

private:
    std::shared_ptr<FrontierTaskSharedState> sharedState;
};
} // namespace function
} // namespace kuzu
