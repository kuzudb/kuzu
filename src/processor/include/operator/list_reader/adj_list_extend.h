#pragma once

#include "src/processor/include/operator/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

class AdjListExtend : public ListReader {

public:
    AdjListExtend(const string& boundVariableName, const string& extensionVariableName,
        const Direction& direction, const label_t& nodeLabel, const label_t& relLabel,
        Operator* prevOperator)
        : ListReader(boundVariableName, extensionVariableName, direction, nodeLabel, relLabel,
              prevOperator) {}

    bool hasNextMorsel();
    void getNextTuples();
    void initialize(Graph* graph);

    Operator* copy() {
        return new AdjListExtend(boundVariableName, extensionVariableName, direction, nodeLabel,
            relLabel, prevOperator->copy());
    }

private:
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<NodeIDVector> outNodeIDVector;
};

} // namespace processor
} // namespace graphflow
