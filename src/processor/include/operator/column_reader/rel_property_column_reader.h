#pragma once

#include "src/processor/include/operator/column_reader/node_property_column_reader.h"

namespace graphflow {
namespace processor {

class RelPropertyColumnReader : public NodePropertyColumnReader {

public:
    RelPropertyColumnReader(const string& boundVariableOrRelName, const label_t& relLabel,
        const label_t& nodeLabel, const string& propertyName)
        : NodePropertyColumnReader{boundVariableOrRelName, nodeLabel, propertyName},
          relLabel(relLabel) {}

    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

protected:
    const label_t relLabel;
};

} // namespace processor
} // namespace graphflow
