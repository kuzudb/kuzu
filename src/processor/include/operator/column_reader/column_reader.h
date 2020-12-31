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
        const string& boundVariableOrRelName, const label_t& nodeLabel, Operator* prevOperator)
        : Operator(prevOperator), boundVariableOrRelName(boundVariableOrRelName),
          nodeLabel(nodeLabel), handle(make_unique<VectorFrameHandle>()) {}

    virtual void initialize(Graph* graph);

    virtual void getNextTuples();

    inline void cleanup() { column->reclaim(handle); }

protected:
    string boundVariableOrRelName;
    label_t nodeLabel;

    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<NodeIDVector> inNodeIDVector;
    shared_ptr<ValueVector> outValueVector;

    BaseColumn* column;
    unique_ptr<VectorFrameHandle> handle;
};

} // namespace processor
} // namespace graphflow
