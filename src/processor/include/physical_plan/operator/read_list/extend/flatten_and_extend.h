#pragma once

#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
class FlattenAndExtend : public AdjListExtend {

public:
    FlattenAndExtend(uint64_t inDataChunkPos, uint64_t inValueVectorPos, AdjLists* lists,
        unique_ptr<PhysicalOperator> prevOperator)
        : AdjListExtend{inDataChunkPos, inValueVectorPos, lists, move(prevOperator)} {};

    void getNextTuples() override {
        if (handle->hasMoreToRead()) {
            readValuesFromList();
            outDataChunk->state->numSelectedValues = outDataChunk->state->size;
            if constexpr (IS_OUT_DATACHUNK_FILTERED) {
                initializeSelector();
            }
            return;
        }
        while (true) {
            if (inDataChunk->state->numSelectedValues == 0ul ||
                inDataChunk->state->numSelectedValues == inDataChunk->state->currPos + 1ul) {
                do {
                    inDataChunk->state->currPos = -1;
                    prevOperator->getNextTuples();
                } while (
                    inDataChunk->state->size > 0 && inDataChunk->state->numSelectedValues == 0);
                if (inDataChunk->state->size == 0) {
                    outDataChunk->state->numSelectedValues = outDataChunk->state->size = 0;
                    return;
                }
            }
            do {
                inDataChunk->state->currPos++;
                readValuesFromList();
            } while (outDataChunk->state->size == 0 &&
                     inDataChunk->state->numSelectedValues > inDataChunk->state->currPos + 1ul);
            if (outDataChunk->state->size > 0) {
                outDataChunk->state->numSelectedValues = outDataChunk->state->size;
                if constexpr (IS_OUT_DATACHUNK_FILTERED) {
                    initializeSelector();
                }
                return;
            }
        }
    }

    void initializeSelector() {
        auto selector = outDataChunk->state->selectedValuesPos.get();
        for (auto i = 0u; i < outDataChunk->state->size; i++) {
            selector[i] = i;
        }
    }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<FlattenAndExtend<IS_OUT_DATACHUNK_FILTERED>>(
            inDataChunkPos, inValueVectorPos, (AdjLists*)lists, prevOperator->clone());
    }
};

} // namespace processor
} // namespace graphflow
