#pragma once

#include "common/types/types.h"
#include "function/gds/gds_task.h"
#include "function/gds/gds_vertex_compute.h"

namespace kuzu {
namespace algo_extension {

class InMemParallelCompute {
public:
    virtual ~InMemParallelCompute() = default;

    virtual void parallelCompute(common::offset_t, common::offset_t) {}

    virtual std::unique_ptr<InMemParallelCompute> copy() = 0;
};

class InMemParallelComputeTask final : public common::Task {
public:
    InMemParallelComputeTask(const uint64_t maxNumThreads, InMemParallelCompute& vc,
        std::shared_ptr<function::VertexComputeTaskSharedState> sharedState)
        : Task{maxNumThreads}, vc{vc}, sharedState{std::move(sharedState)} {};

    function::VertexComputeTaskSharedState* getSharedState() const { return sharedState.get(); }

    void run() override;

private:
    InMemParallelCompute& vc;
    std::shared_ptr<function::VertexComputeTaskSharedState> sharedState;
};

class InMemResultsComputeTask final : public common::Task {
public:
    InMemResultsComputeTask(const uint64_t maxNumThreads, function::GDSResultVertexCompute& vc,
        std::shared_ptr<function::VertexComputeTaskSharedState> sharedState,
        common::table_id_t tableId)
        : Task{maxNumThreads}, vc{vc}, sharedState{std::move(sharedState)}, tableId{tableId} {};

    function::VertexComputeTaskSharedState* getSharedState() const { return sharedState.get(); }

    void run() override;

private:
    function::GDSResultVertexCompute& vc;
    std::shared_ptr<function::VertexComputeTaskSharedState> sharedState;
    common::table_id_t tableId;
};

class InMemGDSUtils {
public:
    static void runParallelCompute(InMemParallelCompute& vc, common::offset_t maxOffset,
        processor::ExecutionContext* context);
    static void runParallelCompute(function::GDSResultVertexCompute& vc, common::offset_t maxOffset,
        processor::ExecutionContext* context, const common::table_id_t& tableId);
};

} // namespace algo_extension
} // namespace kuzu
