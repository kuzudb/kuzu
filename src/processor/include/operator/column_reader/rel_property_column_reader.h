#pragma once

#include "src/processor/include/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

class RelPropertyColumnReader : public ColumnReader {

public:
    RelPropertyColumnReader(const string& relVariableName, const label_t& relLabel,
        const label_t& nodeLabel, const string& propertyName)
        : ColumnReader{relVariableName, nodeLabel}, propertyName{propertyName}, relLabel(relLabel) {
    }
    RelPropertyColumnReader(FileDeserHelper& fdsh);

    void initialize(Graph* graph) override;

    unique_ptr<Operator> clone() override {
        auto copy = make_unique<RelPropertyColumnReader>(
            nodeOrRelVarName, relLabel, nodeLabel, propertyName);
        copy->setPrevOperator(prevOperator->clone());
        return copy;
    }

    void serialize(FileSerHelper& fsh) override;

protected:
    const string propertyName;
    const label_t relLabel;
};

} // namespace processor
} // namespace graphflow
