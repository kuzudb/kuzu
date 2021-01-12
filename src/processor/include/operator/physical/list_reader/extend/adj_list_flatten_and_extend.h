#pragma once

#include "src/processor/include/operator/physical/list_reader/adj_list_extend.h"

namespace graphflow {
namespace processor {

class AdjListFlattenAndExtend : public AdjListExtend {

public:
    AdjListFlattenAndExtend(const uint64_t& inDataChunkIdx, const uint64_t& inValueVectorIdx,
        BaseLists* lists, unique_ptr<Operator> prevOperator)
        : AdjListExtend{inDataChunkIdx, inValueVectorIdx, lists, move(prevOperator)} {};

    bool hasNextMorsel() override;

    void getNextTuples() override;

    unique_ptr<Operator> clone() override {
        return make_unique<AdjListFlattenAndExtend>(
            inDataChunkIdx, inValueVectorIdx, lists, move(prevOperator->clone()));
    }
};

} // namespace processor
} // namespace graphflow
