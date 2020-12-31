#pragma once

#include "src/processor/include/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

class AdjColumnExtend : public ColumnReader {

public:
    AdjColumnExtend(const string& boundVariableOrRelName, const string& extensionVariableName,
        const Direction& direction, const label_t& nodeLabel, const label_t& relLabel,
        Operator* prevOperator)
        : ColumnReader{boundVariableOrRelName, nodeLabel, prevOperator},
          extensionVariableName(extensionVariableName), direction(direction), relLabel(relLabel) {}

    void initialize(Graph* graph);

    Operator* copy() {
        return new AdjColumnExtend(boundVariableOrRelName, extensionVariableName, direction,
            nodeLabel, relLabel, prevOperator->copy());
    }

protected:
    const string extensionVariableName;
    const Direction direction;
    const label_t relLabel;
};

} // namespace processor
} // namespace graphflow
