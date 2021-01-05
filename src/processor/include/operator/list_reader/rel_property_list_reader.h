#pragma once

#include "src/processor/include/operator/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

class RelPropertyListReader : public ListReader {

public:
    RelPropertyListReader(const string& boundNodeVarName, const string& nbrNodeVarName,
        const Direction& direction, const label_t& nodeLabel, const label_t& relLabel,
        const string& propertyName)
        : ListReader(boundNodeVarName, nbrNodeVarName, direction, nodeLabel, relLabel),
          propertyName(propertyName) {}
    RelPropertyListReader(FileDeserHelper& fdsh);

    void initialize(Graph* graph) override;

    void getNextTuples() override;

    unique_ptr<Operator> clone() override {
        auto copy = make_unique<RelPropertyListReader>(
            boundNodeVarName, nbrNodeVarName, direction, nodeLabel, relLabel, propertyName);
        copy->setPrevOperator(prevOperator->clone());
        return copy;
    }

    void serialize(FileSerHelper& fsh) override;

private:
    const string propertyName;
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow
