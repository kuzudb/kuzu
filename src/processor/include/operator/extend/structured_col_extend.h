#pragma once

#include "src/processor/include/operator/property_reader/node/structured_node_property_reader.h"

namespace graphflow {
namespace processor {

class StructuredColumnExtend : public PropertyReader {

public:
    StructuredColumnExtend(Direction direction, label_t srcNodeLabel, label_t relLabel,
        uint64_t propertyIdx, uint64_t nodeVectorIdx, uint64_t dataChunkIdx)
        : PropertyReader{nodeVectorIdx, dataChunkIdx}, direction(direction),
          srcNodeLabel(srcNodeLabel), relLabel(relLabel) {}

    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

protected:
    Direction direction;
    label_t srcNodeLabel;
    label_t relLabel;
};

} // namespace processor
} // namespace graphflow
