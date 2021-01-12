#pragma once

#include "src/processor/include/operator/physical/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

class AdjListExtend : public ListReader {

public:
    AdjListExtend(const uint64_t& inDataChunkIdx, const uint64_t& inValueVectorIdx,
        BaseLists* lists, unique_ptr<Operator> prevOperator);

protected:
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<NodeIDVector> outNodeIDVector;
};

} // namespace processor
} // namespace graphflow
