#pragma once

#include "src/processor/include/operator/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

class AdjListExtend : public ListReader {

public:
    AdjListExtend(const string& boundNodeVarName, const string& nbrNodeVarName,
        const Direction& direction, const label_t& nodeLabel, const label_t& relLabel)
        : ListReader(boundNodeVarName, nbrNodeVarName, direction, nodeLabel, relLabel) {}
    AdjListExtend(FileDeserHelper& fdsh);

    void initialize(Graph* graph) override;

    bool hasNextMorsel() override;

    void getNextTuples() override;

    unique_ptr<Operator> clone() override {
        auto copy = make_unique<AdjListExtend>(
            boundNodeVarName, nbrNodeVarName, direction, nodeLabel, relLabel);
        copy->setPrevOperator(prevOperator->clone());
        return copy;
    }

    void serialize(FileSerHelper& fsh) override;

private:
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<NodeIDVector> outNodeIDVector;
};

} // namespace processor
} // namespace graphflow
