#pragma once

#include "src/processor/include/operator/column_reader/column_reader.h"

namespace graphflow {
namespace processor {

class AdjColumnExtend : public ColumnReader {

public:
    AdjColumnExtend(const string& boundNodeVarName, const string& nbrNodeVarName,
        const Direction& direction, const label_t& nodeLabel, const label_t& relLabel)
        : ColumnReader{boundNodeVarName, nodeLabel}, nbrNodeVarName(nbrNodeVarName),
          direction(direction), relLabel(relLabel) {}
    AdjColumnExtend(FileDeserHelper& fdsh);

    void initialize(Graph* graph) override;

    unique_ptr<Operator> clone() override {
        auto copy = make_unique<AdjColumnExtend>(
            nodeOrRelVarName, nbrNodeVarName, direction, nodeLabel, relLabel);
        copy->setPrevOperator(prevOperator->clone());
        return copy;
    }

    void serialize(FileSerHelper& fsh) override;

protected:
    const string nbrNodeVarName;
    const Direction direction;
    const label_t relLabel;
};

} // namespace processor
} // namespace graphflow
