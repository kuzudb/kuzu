#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/operator/physical/operator.h"

namespace graphflow {
namespace processor {

class ListReader : public Operator {

public:
    ListReader(const uint64_t& inDataChunkIdx, const uint64_t& inValueVectorIdx, BaseLists* lists,
        unique_ptr<Operator> prevOperator);

    void cleanup() override;

protected:
    uint64_t inDataChunkIdx;
    uint64_t inValueVectorIdx;
    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<NodeIDVector> inNodeIDVector;

    BaseLists* lists;
    unique_ptr<VectorFrameHandle> handle;
};

} // namespace processor
} // namespace graphflow
