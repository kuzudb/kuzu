#pragma once

#include "src/processor/include/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

class NodePropertyColumnReader : public ColumnReader {

public:
    NodePropertyColumnReader(
        const string& nodeVarName, const label_t& nodeLabel, const string& propertyName)
        : ColumnReader{nodeVarName, nodeLabel}, propertyName(propertyName) {}
    NodePropertyColumnReader(FileDeserHelper& fdsh);

    void initialize(Graph* graph) override;

    unique_ptr<Operator> clone() override {
        auto copy =
            make_unique<NodePropertyColumnReader>(nodeOrRelVarName, nodeLabel, propertyName);
        copy->setPrevOperator(prevOperator->clone());
        return copy;
    }

    void serialize(FileSerHelper& fsh) override;

protected:
    const string propertyName;
};

} // namespace processor
} // namespace graphflow
