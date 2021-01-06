#pragma once

#include "src/processor/include/operator/physical/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

class AdjListExtend : public ListReader {

public:
    AdjListExtend(const uint64_t& inDataChunkIdx, const uint64_t& inValueVectorIdx,
        BaseLists* lists, unique_ptr<Operator> prevOperator);

    bool hasNextMorsel() override;

    void getNextTuples() override;

    unique_ptr<Operator> clone() override {
        return make_unique<AdjListExtend>(
            inDataChunkIdx, inValueVectorIdx, lists, move(prevOperator->clone()));
    }

private:
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<NodeIDVector> outNodeIDVector;
};

} // namespace processor
} // namespace graphflow
