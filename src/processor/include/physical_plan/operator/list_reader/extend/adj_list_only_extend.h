#pragma once

#include "src/processor/include/physical_plan/operator/list_reader/adj_list_extend.h"

namespace graphflow {
namespace processor {

class AdjListOnlyExtend : public AdjListExtend {

public:
    AdjListOnlyExtend(const uint64_t& inDataChunkPos, const uint64_t& inValueVectorPos,
        BaseLists* lists, unique_ptr<PhysicalOperator> prevOperator)
        : AdjListExtend{inDataChunkPos, inValueVectorPos, lists, move(prevOperator)} {};

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjListOnlyExtend>(
            inDataChunkPos, inValueVectorPos, lists, prevOperator->clone());
    }
};

} // namespace processor
} // namespace graphflow
