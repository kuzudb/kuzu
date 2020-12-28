#pragma once

#include "src/processor/include/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

class AdjColumnReader : public ColumnReader {

public:
    AdjColumnReader(const string& boundVariableOrRelName, const string& extensionVariableName,
        const Direction& direction, const label_t& nodeLabel, const label_t& relLabel)
        : ColumnReader{boundVariableOrRelName, nodeLabel},
          extensionVariableName(extensionVariableName), direction(direction), relLabel(relLabel) {}

    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

protected:
    const string extensionVariableName;
    const Direction direction;
    const label_t relLabel;
};

} // namespace processor
} // namespace graphflow
