#pragma once

#include <memory>

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/operator/operator.h"
#include "src/processor/include/task_system/morsel.h"

namespace graphflow {
namespace processor {

class PropertyReader : public Operator {

public:
    PropertyReader(uint64_t nodeVectorIdx, uint64_t dataChunkIdx)
        : dataChunkIdx(dataChunkIdx), nodeIDVectorIdx(nodeVectorIdx),
          handle(make_unique<VectorFrameHandle>()) {}

    virtual void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

    virtual void getNextTuples();

    void cleanup() { column->reclaim(handle); }

    shared_ptr<ValueVector>& getPropertyVector() { return outValueVector; }

protected:
    uint64_t dataChunkIdx;
    shared_ptr<DataChunk> outDataChunk;

    Column* column;

    shared_ptr<BaseNodeIDVector> nodeIDVector;
    uint64_t nodeIDVectorIdx;

    shared_ptr<ValueVector> outValueVector;
    unique_ptr<VectorFrameHandle> handle;
};

} // namespace processor
} // namespace graphflow
