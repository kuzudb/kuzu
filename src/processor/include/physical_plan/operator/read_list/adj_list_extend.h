#pragma once

#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

class AdjListExtend : public ReadList {

public:
    AdjListExtend(uint32_t inDataChunkPos, uint32_t inValueVectorPos, uint32_t outDataChunkSize,
        uint32_t outDataChunkPos, uint32_t outValueVectorPos, AdjLists* lists,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjListExtend>(inDataChunkPos, inValueVectorPos, outDataChunkSize,
            outDataChunkPos, outValueVectorPos, (AdjLists*)lists, prevOperator->clone(), context,
            id);
    }

private:
    uint32_t outDataChunkSize;
};

} // namespace processor
} // namespace graphflow
