#pragma once

#include "src/processor/include/physical_plan/operator/list_reader/adj_list_extend.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
class AdjListFlattenAndExtend : public AdjListExtend {

public:
    AdjListFlattenAndExtend(uint64_t inDataChunkPos, uint64_t inValueVectorPos, BaseLists* lists,
        unique_ptr<PhysicalOperator> prevOperator)
        : AdjListExtend{inDataChunkPos, inValueVectorPos, lists, move(prevOperator)} {};

    bool hasNextMorsel() override {
        return (inDataChunk->numSelectedValues > 0ul &&
                   inDataChunk->numSelectedValues > inDataChunk->currPos + 1l) ||
               handle->hasMoreToRead() || prevOperator->hasNextMorsel();
    }

    void getNextTuples() override {
        if (handle->hasMoreToRead()) {
            readValuesFromList();
            return;
        }
        if (inDataChunk->numSelectedValues == 0ul ||
            inDataChunk->numSelectedValues == inDataChunk->currPos + 1ul) {
            inDataChunk->currPos = -1;
            prevOperator->getNextTuples();
            if (inDataChunk->size == 0) {
                outDataChunk->size = 0;
                return;
            }
        }
        inDataChunk->currPos++;
        readValuesFromList();
        outDataChunk->numSelectedValues = outDataChunk->size;
        if constexpr (IS_OUT_DATACHUNK_FILTERED) {
            auto selector = outDataChunk->selectedValuesPos.get();
            for (auto i = 0u; i < outDataChunk->size; i++) {
                selector[i] = i;
            }
        }
    }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjListFlattenAndExtend<IS_OUT_DATACHUNK_FILTERED>>(
            inDataChunkPos, inValueVectorPos, lists, prevOperator->clone());
    }
};

} // namespace processor
} // namespace graphflow
