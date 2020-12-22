#pragma once

#include <memory>

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/operator/operator.h"
#include "src/processor/include/task_system/morsel.h"

namespace graphflow {
namespace processor {

class ColumnReader : public Operator {

public:
    ColumnReader(
        const label_t& nodeLabel, const uint64_t& dataChunkIdx, const uint64_t& nodeVectorIdx)
        : nodeLabel(nodeLabel), dataChunkIdx(dataChunkIdx), nodeIDVectorIdx(nodeVectorIdx),
          handle(make_unique<VectorFrameHandle>()) {}

    virtual void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

    virtual void getNextTuples();

    inline void cleanup() { column->reclaim(handle); }

    shared_ptr<ValueVector>& getPropertyVector() { return outValueVector; }

protected:
    label_t nodeLabel;

    uint64_t dataChunkIdx;
    shared_ptr<DataChunk> outDataChunk;

    shared_ptr<NodeIDVector> nodeIDVector;
    uint64_t nodeIDVectorIdx;

    shared_ptr<ValueVector> outValueVector;
    unique_ptr<VectorFrameHandle> handle;

    BaseColumn* column;
};

} // namespace processor
} // namespace graphflow
