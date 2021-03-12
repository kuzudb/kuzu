#pragma once

#include "src/processor/include/operator/physical/list_reader/adj_list_extend.h"

namespace graphflow {
namespace processor {

class AdjListFlattenAndExtend : public AdjListExtend {

public:
    AdjListFlattenAndExtend(const uint64_t& inDataChunkPos, const uint64_t& inValueVectorPos,
        BaseLists* lists, unique_ptr<Operator> prevOperator)
        : AdjListExtend{inDataChunkPos, inValueVectorPos, lists, move(prevOperator)} {};

    bool hasNextMorsel() override;

    void getNextTuples() override;

    unique_ptr<Operator> clone() override {
        return make_unique<AdjListFlattenAndExtend>(
            inDataChunkPos, inValueVectorPos, lists, prevOperator->clone());
    }
};

} // namespace processor
} // namespace graphflow
