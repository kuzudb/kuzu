#pragma once

#include "src/processor/include/operator/column_reader/node_property_reader.h"

namespace graphflow {
namespace processor {

class AdjColumnPropertyReader : public NodePropertyReader {

public:
    AdjColumnPropertyReader(const label_t& relLabel, const label_t& nodeLabel,
        const string& propertyName, const uint64_t& nodeVectorIdx, const uint64_t& dataChunkIdx)
        : NodePropertyReader{nodeLabel, propertyName, nodeVectorIdx, dataChunkIdx},
          relLabel(relLabel) {}

    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

protected:
    const label_t relLabel;
};

} // namespace processor
} // namespace graphflow
