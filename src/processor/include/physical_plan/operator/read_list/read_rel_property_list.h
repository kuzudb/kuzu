#pragma once

#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

class ReadRelPropertyList : public ReadList {

public:
    ReadRelPropertyList(uint32_t inDataChunkPos, uint32_t inValueVectorPos,
        uint32_t outDataChunkPos, uint32_t outValueVectorPos, Lists* lists,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ReadRelPropertyList>(inDataChunkPos, inValueVectorPos, outDataChunkPos,
            outValueVectorPos, lists, prevOperator->clone(), context, id);
    }
};

} // namespace processor
} // namespace graphflow
