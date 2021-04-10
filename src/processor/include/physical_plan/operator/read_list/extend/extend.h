#pragma once

#include "src/processor/include/physical_plan/operator/read_list/adj_list_extend.h"

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
class Extend : public AdjListExtend {

public:
    Extend(const uint64_t& inDataChunkPos, const uint64_t& inValueVectorPos, BaseLists* lists,
        unique_ptr<PhysicalOperator> prevOperator)
        : AdjListExtend{inDataChunkPos, inValueVectorPos, lists, move(prevOperator)} {};

    void getNextTuples() override {
        if (handle->hasMoreToRead()) {
            readValuesFromList();
            outDataChunk->numSelectedValues = outDataChunk->size;
            if constexpr (IS_OUT_DATACHUNK_FILTERED) {
                initializeSelector();
            }
            return;
        }
        while (true) {
            do {
                prevOperator->getNextTuples();
            } while (inDataChunk->size > 0 && inDataChunk->numSelectedValues == 0);
            if (inDataChunk->size > 0) {
                readValuesFromList();
                if (outDataChunk->size > 0) {
                    outDataChunk->numSelectedValues = outDataChunk->size;
                    if constexpr (IS_OUT_DATACHUNK_FILTERED) {
                        initializeSelector();
                    }
                    return;
                }
            } else {
                outDataChunk->size = outDataChunk->numSelectedValues = 0;
                return;
            }
        }
    }

    void initializeSelector() {
        auto selector = outDataChunk->selectedValuesPos.get();
        for (auto i = 0u; i < outDataChunk->size; i++) {
            selector[i] = i;
        }
    }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Extend<IS_OUT_DATACHUNK_FILTERED>>(
            inDataChunkPos, inValueVectorPos, lists, prevOperator->clone());
    }
};

} // namespace processor
} // namespace graphflow
