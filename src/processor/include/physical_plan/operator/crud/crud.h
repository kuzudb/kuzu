#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class CRUDOperator : public PhysicalOperator {

public:
    CRUDOperator(uint64_t dataChunkPos, PhysicalOperatorType type,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id);

    void getNextTuples() override;

protected:
    uint64_t dataChunkPos;
    shared_ptr<DataChunk> inDataChunk;
};

} // namespace processor
} // namespace graphflow
