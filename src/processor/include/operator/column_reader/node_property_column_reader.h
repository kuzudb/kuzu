#pragma once

#include "src/processor/include/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

class NodePropertyColumnReader : public ColumnReader {

public:
    NodePropertyColumnReader(const string& boundVariableOrRelName, const label_t& nodeLabel,
        const string& propertyName, Operator* prevOperator)
        : ColumnReader{boundVariableOrRelName, nodeLabel, prevOperator},
          propertyName(propertyName) {}

    virtual void initialize(Graph* graph);

    Operator* copy() {
        return new NodePropertyColumnReader(
            boundVariableOrRelName, nodeLabel, propertyName, prevOperator->copy());
    }

protected:
    const string propertyName;
};

} // namespace processor
} // namespace graphflow
