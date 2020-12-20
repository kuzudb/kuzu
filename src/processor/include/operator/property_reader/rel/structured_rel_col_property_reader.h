#pragma once

#include "src/processor/include/operator/property_reader/node/structured_node_property_reader.h"

namespace graphflow {
namespace processor {

class StructuredRelColPropertyReader : public StructuredNodePropertyReader {

public:
    StructuredRelColPropertyReader(label_t relLabel, label_t nodeLabel, uint64_t propertyKey,
        uint64_t nodeVectorIdx, uint64_t dataChunkIdx)
        : StructuredNodePropertyReader{nodeLabel, propertyKey, nodeVectorIdx, dataChunkIdx},
          relLabel(relLabel) {}

    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

protected:
    label_t relLabel;
};

} // namespace processor
} // namespace graphflow
