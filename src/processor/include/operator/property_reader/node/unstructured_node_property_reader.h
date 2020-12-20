#pragma once

#include "src/processor/include/operator/property_reader/property_reader.h"

namespace graphflow {
namespace processor {

class UnstructuredNodePropertyReader : public PropertyReader {

public:
    UnstructuredNodePropertyReader(
        uint64_t propertyKey, uint64_t nodeVectorIdx, uint64_t dataChunkIdx)
        : PropertyReader{nodeVectorIdx, dataChunkIdx}, propertyKey(propertyKey) {}

    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

    virtual void getNextTuples();

private:
    Graph* graph;
    uint64_t propertyKey;
};

} // namespace processor
} // namespace graphflow
