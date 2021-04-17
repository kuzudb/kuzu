#pragma once

#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
class Extend : public AdjListExtend {

public:
    Extend(const uint64_t& inDataChunkPos, const uint64_t& inValueVectorPos, AdjLists* lists,
        unique_ptr<PhysicalOperator> prevOperator)
        : AdjListExtend{inDataChunkPos, inValueVectorPos, lists, move(prevOperator)} {};

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Extend<IS_OUT_DATACHUNK_FILTERED>>(
            inDataChunkPos, inValueVectorPos, (AdjLists*)lists, prevOperator->clone());
    }
};

} // namespace processor
} // namespace graphflow
