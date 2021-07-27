#pragma once

#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

class AdjListExtend : public ReadList {

public:
    AdjListExtend(uint64_t inDataChunkPos, uint64_t inValueVectorPos, AdjLists* lists,
        label_t outNodeIDVectorLabel, unique_ptr<PhysicalOperator> prevOperator,
        ExecutionContext& context, uint32_t id);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjListExtend>(inDataChunkPos, inValueVectorPos, (AdjLists*)lists,
            outNodeIDVectorLabel, prevOperator->clone(), context, id);
    }

private:
    label_t outNodeIDVectorLabel;
};

} // namespace processor
} // namespace graphflow
