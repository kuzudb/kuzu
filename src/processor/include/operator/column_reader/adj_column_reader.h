#pragma once

#include "src/processor/include/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

class AdjColumnReader : public ColumnReader {

public:
    AdjColumnReader(const Direction& direction, const label_t& nodeLabel, const label_t& relLabel,
        const uint64_t& propertyIdx, const uint64_t& nodeVectorIdx, const uint64_t& dataChunkIdx)
        : ColumnReader{nodeLabel, nodeVectorIdx, dataChunkIdx}, direction(direction),
          relLabel(relLabel) {}

    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

protected:
    const Direction direction;
    const label_t relLabel;
};

} // namespace processor
} // namespace graphflow
