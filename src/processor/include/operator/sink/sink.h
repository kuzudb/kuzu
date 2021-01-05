#pragma once

#include "src/processor/include/operator/operator.h"

namespace graphflow {
namespace processor {

class Sink : public Operator {
public:
    Sink() = default;
    Sink(FileDeserHelper& fdsh);

    void initialize(Graph* graph) override { prevOperator->initialize(graph); }

    void getNextTuples() override;

    unique_ptr<Operator> clone() override {
        auto copy = make_unique<Sink>();
        copy->setPrevOperator(prevOperator->clone());
        return copy;
    }

    void serialize(FileSerHelper& fsh);

    uint64_t getNumTuples() { return numTuples; }

protected:
    uint64_t numTuples{0};
};

} // namespace processor
} // namespace graphflow
