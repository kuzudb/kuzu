#pragma once

#include "src/processor/include/operator/column_reader/node_property_column_reader.h"

namespace graphflow {
namespace processor {

class RelPropertyColumnReader : public NodePropertyColumnReader {

public:
    RelPropertyColumnReader(const label_t& relLabel, const label_t& nodeLabel,
        const string& propertyName, const uint64_t& nodeVectorIdx, const uint64_t& dataChunkIdx)
        : NodePropertyColumnReader{nodeLabel, propertyName, nodeVectorIdx, dataChunkIdx},
          relLabel(relLabel) {}

    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

protected:
    const label_t relLabel;
};

} // namespace processor
} // namespace graphflow
