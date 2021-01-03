#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/operator/operator.h"

namespace graphflow {
namespace processor {

class ListReader : public Operator {

public:
    ListReader(FileDeserHelper& fdsh);
    ListReader(const string& boundNodeVarName, const string& nbrNodeVarName,
        const Direction& direction, const label_t& nodeLabel, const label_t& relLabel)
        : boundNodeVarName{boundNodeVarName}, nbrNodeVarName{nbrNodeVarName}, direction{direction},
          nodeLabel{nodeLabel}, relLabel{relLabel} {}

    void initialize(Graph* graph) override;

    void cleanup() override;

    void serialize(FileSerHelper& fsh) override;

protected:
    const string boundNodeVarName;
    const string nbrNodeVarName;
    const Direction direction;
    const label_t nodeLabel;
    const label_t relLabel;

    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<NodeIDVector> inNodeIDVector;

    BaseLists* lists;
    unique_ptr<VectorFrameHandle> handle = make_unique<VectorFrameHandle>();
};

} // namespace processor
} // namespace graphflow
