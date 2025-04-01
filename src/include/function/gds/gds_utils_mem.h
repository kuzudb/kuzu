#pragma once

#include "gds_task.h"
#include "graph/graph_mem.h"

namespace kuzu {
namespace function {

class InMemVertexCompute {
public:
    virtual ~InMemVertexCompute() = default;

    virtual void vertexCompute(common::offset_t, common::offset_t) {}

    virtual std::unique_ptr<InMemVertexCompute> copy() = 0;
};

class InMemVertexComputeTask : public common::Task {
public:
    InMemVertexComputeTask(uint64_t maxNumThreads, InMemVertexCompute& vc,
        std::shared_ptr<VertexComputeTaskSharedState> sharedState)
        : common::Task{maxNumThreads}, vc{vc}, sharedState{std::move(sharedState)} {};

    VertexComputeTaskSharedState* getSharedState() const { return sharedState.get(); }

    void run() override;

private:
    InMemVertexCompute& vc;
    std::shared_ptr<VertexComputeTaskSharedState> sharedState;
};

class GDSUtilsInMemory {
public:
    static void runVertexCompute(InMemVertexCompute& vc, graph::InMemoryGraph& graph,
        processor::ExecutionContext* context);
};

} // namespace function
} // namespace kuzu
