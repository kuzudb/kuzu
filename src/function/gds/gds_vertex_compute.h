#pragma once

#include "function/gds/compute.h"
#include "processor/operator/gds_call_shared_state.h"

namespace kuzu {
namespace function {

class GDSVertexCompute : public VertexCompute {
public:
    explicit GDSVertexCompute(common::NodeOffsetMaskMap* nodeMask) : nodeMask{nodeMask} {}

    bool beginOnTable(common::table_id_t tableID) override {
        if (nodeMask != nullptr) {
            nodeMask->pin(tableID);
        }
        beginOnTableInternal(tableID);
        return true;
    }

protected:
    bool skip(common::offset_t offset) {
        if (nodeMask != nullptr) {
            return !nodeMask->valid(offset);
        }
        return false;
    }

    virtual void beginOnTableInternal(common::table_id_t tableID) = 0;

protected:
    common::NodeOffsetMaskMap* nodeMask;
};

class GDSResultVertexCompute : public GDSVertexCompute {
public:
    GDSResultVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState)
        : GDSVertexCompute{sharedState->getGraphNodeMaskMap()}, sharedState{sharedState}, mm{mm} {
        localFT = sharedState->factorizedTablePool.claimLocalTable(mm);
    }
    ~GDSResultVertexCompute() override {
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
    processor::GDSCallSharedState* sharedState;
    storage::MemoryManager* mm;
    processor::FactorizedTable* localFT = nullptr;
    std::vector<common::ValueVector*> vectors;
};

} // namespace function
} // namespace kuzu
