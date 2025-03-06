#pragma once

#include "function/gds/compute.h"

namespace kuzu {
namespace function {

class GDSVertexCompute : public VertexCompute {
public:
    explicit GDSVertexCompute(processor::NodeOffsetMaskMap* nodeMask) : nodeMask{nodeMask} {}

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
    processor::NodeOffsetMaskMap* nodeMask;
};

class GDSResultVertexCompute : public GDSVertexCompute {
public:
    GDSResultVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState)
        : GDSVertexCompute{sharedState->getGraphNodeMaskMap()}, sharedState{sharedState}, mm{mm} {
        localFT = sharedState->claimLocalTable(mm);
    }
    ~GDSResultVertexCompute() override { sharedState->returnLocalTable(localFT); }

protected:
    processor::GDSCallSharedState* sharedState;
    storage::MemoryManager* mm;
    processor::FactorizedTable* localFT;
};

} // namespace function
} // namespace kuzu
