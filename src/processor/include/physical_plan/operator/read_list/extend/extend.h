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
            do {
                prevOperator->getNextTuples();
            } while (inDataChunk->state->size > 0 && inDataChunk->state->numSelectedValues == 0);
            if (inDataChunk->state->size > 0) {
                readValuesFromList();
                if (outDataChunk->state->size > 0) {
                    outDataChunk->state->numSelectedValues = outDataChunk->state->size;
                    if constexpr (IS_OUT_DATACHUNK_FILTERED) {
                        initializeSelector();
                    }
                    return;
                }
            } else {
                outDataChunk->state->size = outDataChunk->state->numSelectedValues = 0;
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
        return make_unique<Extend<IS_OUT_DATACHUNK_FILTERED>>(
            inDataChunkPos, inValueVectorPos, (AdjLists*)lists, prevOperator->clone());
    }
};

} // namespace processor
} // namespace graphflow
