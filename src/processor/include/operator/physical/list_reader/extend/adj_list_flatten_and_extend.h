#pragma once

#include "src/processor/include/operator/physical/list_reader/adj_list_extend.h"

namespace graphflow {
namespace processor {

class AdjListFlattenAndExtend : public AdjListExtend {

public:
    AdjListFlattenAndExtend(const uint64_t& dataChunkPos, const uint64_t& valueVectorPos,
        BaseLists* lists, unique_ptr<Operator> prevOperator)
        : AdjListExtend{dataChunkPos, valueVectorPos, lists, move(prevOperator)} {};

    bool hasNextMorsel() override;

    void getNextTuples() override;

    unique_ptr<Operator> clone() override {
        return make_unique<AdjListFlattenAndExtend>(
            dataChunkPos, valueVectorPos, lists, prevOperator->clone());
    }
};

} // namespace processor
} // namespace graphflow
