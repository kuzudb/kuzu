#pragma once

#include <memory>

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/operator/operator.h"
#include "src/processor/include/task_system/morsel.h"

namespace graphflow {
namespace processor {

class ColumnReader : public Operator {

public:
    ColumnReader(const string& nodeOrRelVarName, const label_t& nodeLabel)
        : nodeOrRelVarName{nodeOrRelVarName}, nodeLabel{nodeLabel} {}
    ColumnReader(FileDeserHelper& fdsh);

    void initialize(Graph* graph) override;

    void getNextTuples() override;

    inline void cleanup() override { column->reclaim(handle); }

    void serialize(FileSerHelper& fsh) override;

protected:
    const string nodeOrRelVarName;
    const label_t nodeLabel;

    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<NodeIDVector> inNodeIDVector;
    shared_ptr<ValueVector> outValueVector;

    BaseColumn* column;
    unique_ptr<VectorFrameHandle> handle = make_unique<VectorFrameHandle>();
};

} // namespace processor
} // namespace graphflow
