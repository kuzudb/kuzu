#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/operator/operator.h"

namespace graphflow {
namespace processor {

class Extend : public Operator {

public:
    Extend(const string& boundVariableName, const string& extensionVariableName,
        const Direction& direction, const label_t& nodeLabel, const label_t& relLabel)
        : boundVariableName(boundVariableName), extensionVariableName(extensionVariableName),
          direction(direction), nodeLabel(nodeLabel), relLabel(relLabel),
          handle(make_unique<VectorFrameHandle>()) {}

    bool hasNextMorsel();
    void getNextTuples();
    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

    void cleanup();

private:
    string boundVariableName;
    string extensionVariableName;
    Direction direction;
    label_t nodeLabel;
    label_t relLabel;

    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<DataChunk> outDataChunk;

    shared_ptr<NodeIDVector> inNodeIDVector;
    shared_ptr<NodeIDVector> outNodeIDVector;

    AdjLists* adjLists;
    unique_ptr<VectorFrameHandle> handle;
};

} // namespace processor
} // namespace graphflow
