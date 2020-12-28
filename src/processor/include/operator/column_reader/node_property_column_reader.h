#pragma once

#include "src/processor/include/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

class NodePropertyColumnReader : public ColumnReader {

public:
    NodePropertyColumnReader(
        const string& boundVariableOrRelName, const label_t& nodeLabel, const string& propertyName)
        : ColumnReader{boundVariableOrRelName, nodeLabel}, propertyName(propertyName) {}

    virtual void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

protected:
    const string propertyName;
};

} // namespace processor
} // namespace graphflow
