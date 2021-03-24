#pragma once

#include "src/processor/include/physical_plan/operator/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

class RelPropertyListReader : public ListReader {

public:
    RelPropertyListReader(uint64_t inDataChunkPos, uint64_t inValueVectorPos,
        uint64_t outDataChunkPos, BaseLists* lists, unique_ptr<PhysicalOperator> prevOperator);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<RelPropertyListReader>(
            inDataChunkPos, inValueVectorPos, outDataChunkPos, lists, prevOperator->clone());
    }

private:
    uint64_t outDataChunkPos;
};

} // namespace processor
} // namespace graphflow
