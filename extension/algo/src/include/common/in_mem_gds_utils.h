#pragma once

#include "common/types/types.h"
#include "function/gds/gds.h"
#include "function/gds/gds_task.h"

namespace kuzu {
namespace algo_extension {

class InMemVertexCompute {
public:
    virtual ~InMemVertexCompute() = default;

    virtual void vertexCompute(common::offset_t, common::offset_t, const std::optional<common::table_id_t>) {}

    virtual std::unique_ptr<InMemVertexCompute> copy() = 0;
};

class InMemResultParallelCompute : public InMemVertexCompute {
public:
    InMemResultParallelCompute(storage::MemoryManager* mm, function::GDSFuncSharedState* sharedState)
        : sharedState{sharedState}, mm{mm} {
        localFT = sharedState->factorizedTablePool.claimLocalTable(mm);
    }
    ~InMemResultParallelCompute() override {
        sharedState->factorizedTablePool.returnLocalTable(localFT);
    }

protected:
    std::unique_ptr<common::ValueVector> createVector(const common::LogicalType& type) {
        auto vector = std::make_unique<common::ValueVector>(type.copy(), mm);
        vector->state = common::DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(vector.get());
        return vector;
    }

protected:
    function::GDSFuncSharedState* sharedState;
    storage::MemoryManager* mm;
    processor::FactorizedTable* localFT = nullptr;
    std::vector<common::ValueVector*> vectors;
};

class InMemVertexComputeTask final : public common::Task {
public:
    InMemVertexComputeTask(const uint64_t maxNumThreads, InMemVertexCompute& vc,
        std::shared_ptr<function::VertexComputeTaskSharedState> sharedState, std::optional<common::table_id_t> tableId)
        : Task{maxNumThreads}, vc{vc}, sharedState{std::move(sharedState)}, tableId{tableId} {};

    function::VertexComputeTaskSharedState* getSharedState() const { return sharedState.get(); }

    void run() override;

private:
    InMemVertexCompute& vc;
    std::shared_ptr<function::VertexComputeTaskSharedState> sharedState;
    std::optional<common::table_id_t> tableId;
};

class InMemGDSUtils {
public:
    static void runVertexCompute(InMemVertexCompute& vc, common::offset_t maxOffset,
        processor::ExecutionContext* context, std::optional<common::table_id_t> tableId = std::nullopt);
};

} // namespace algo_extension
} // namespace kuzu
