#pragma once

#include "src/processor/include/operator/property_reader/property_reader.h"

namespace graphflow {
namespace processor {

class StructuredNodePropertyReader : public PropertyReader {

public:
    StructuredNodePropertyReader(
        label_t nodeLabel, uint64_t propertyKey, uint64_t nodeVectorIdx, uint64_t dataChunkIdx)
        : PropertyReader{nodeVectorIdx, dataChunkIdx}, nodeLabel(nodeLabel),
          propertyKey(propertyKey) {}

    virtual void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

protected:
    label_t nodeLabel;
    uint64_t propertyKey;
};

} // namespace processor
} // namespace graphflow
