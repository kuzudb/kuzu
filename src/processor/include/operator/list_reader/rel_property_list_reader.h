#pragma once

#include "src/processor/include/operator/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

class RelPropertyListReader : public ListReader {

public:
    RelPropertyListReader(const string& boundVariableName, const string& extensionVariableName,
        const Direction& direction, const label_t& nodeLabel, const label_t& relLabel,
        const string& propertyName, Operator* prevOperator)
        : ListReader(boundVariableName, extensionVariableName, direction, nodeLabel, relLabel,
              prevOperator),
          propertyName(propertyName) {}

    void getNextTuples();
    void initialize(Graph* graph);

    Operator* copy() {
        return new RelPropertyListReader(boundVariableName, extensionVariableName, direction,
            nodeLabel, relLabel, propertyName, prevOperator->copy());
    }

private:
    const string propertyName;
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow
