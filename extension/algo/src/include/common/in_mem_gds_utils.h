#pragma once

#include "function/gds/gds_task.h"

namespace kuzu {
namespace algo_extension {

class InMemVertexCompute {
public:
    virtual ~InMemVertexCompute() = default;

    virtual void vertexCompute(common::offset_t, common::offset_t) {}

    virtual std::unique_ptr<InMemVertexCompute> copy() = 0;
};

class InMemVertexComputeTask final : public common::Task {
public:
    InMemVertexComputeTask(const uint64_t maxNumThreads, InMemVertexCompute& vc,
        std::shared_ptr<function::VertexComputeTaskSharedState> sharedState)
        : Task{maxNumThreads}, vc{vc}, sharedState{std::move(sharedState)} {};

    function::VertexComputeTaskSharedState* getSharedState() const { return sharedState.get(); }

    void run() override;

private:
    InMemVertexCompute& vc;
    std::shared_ptr<function::VertexComputeTaskSharedState> sharedState;
};

class InMemGDSUtils {
public:
    static void runVertexCompute(InMemVertexCompute& vc, common::offset_t maxOffset,
        processor::ExecutionContext* context);
};

} // namespace algo_extension
} // namespace kuzu
