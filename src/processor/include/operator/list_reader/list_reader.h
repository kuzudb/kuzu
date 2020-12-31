#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/operator/operator.h"

namespace graphflow {
namespace processor {

class ListReader : public Operator {

public:
    ListReader(const string& boundVariableName, const string& extensionVariableName,
        const Direction& direction, const label_t& nodeLabel, const label_t& relLabel,
        Operator* prevOperator)
        : Operator(prevOperator), boundVariableName(boundVariableName),
          extensionVariableName(extensionVariableName), direction(direction), nodeLabel(nodeLabel),
          relLabel(relLabel), handle(make_unique<VectorFrameHandle>()) {}

    virtual void initialize(Graph* graph);

    void cleanup();

protected:
    string boundVariableName;
    string extensionVariableName;
    Direction direction;
    label_t nodeLabel;
    label_t relLabel;

    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<NodeIDVector> inNodeIDVector;

    BaseLists* lists;
    unique_ptr<VectorFrameHandle> handle;
};

} // namespace processor
} // namespace graphflow
