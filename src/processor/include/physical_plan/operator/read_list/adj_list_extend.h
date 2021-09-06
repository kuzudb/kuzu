#pragma once

#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

class AdjListExtend : public ReadList {

public:
    AdjListExtend(uint64_t inDataChunkPos, uint64_t inValueVectorPos, AdjLists* lists,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjListExtend>(
            inDataChunkPos, inValueVectorPos, (AdjLists*)lists, prevOperator->clone(), context, id);
    }
};

} // namespace processor
} // namespace graphflow
