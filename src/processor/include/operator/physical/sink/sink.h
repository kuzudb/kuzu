#pragma once

#include "src/processor/include/operator/physical/operator.h"

namespace graphflow {
namespace processor {

class Sink : public Operator {

public:
    Sink(unique_ptr<Operator> prevOperator) : Operator{move(prevOperator)} {};

    void getNextTuples() override;

    void cleanup() override { prevOperator->cleanup(); };

    unique_ptr<Operator> clone() override { return make_unique<Sink>(move(prevOperator->clone())); }

    uint64_t getNumTuples() { return numTuples; }

protected:
    uint64_t numTuples{0};
};

} // namespace processor
} // namespace graphflow
