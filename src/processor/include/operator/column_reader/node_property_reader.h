#pragma once

#include "src/processor/include/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

class NodePropertyReader : public ColumnReader {

public:
    NodePropertyReader(const label_t& nodeLabel, const string& propertyName,
        const uint64_t& nodeVectorIdx, const uint64_t& dataChunkIdx)
        : ColumnReader{nodeLabel, nodeVectorIdx, dataChunkIdx}, propertyName(propertyName) {}

    virtual void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

protected:
    const string propertyName;
};

} // namespace processor
} // namespace graphflow
