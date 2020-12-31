#pragma once

#include "src/processor/include/operator/operator.h"

namespace graphflow {
namespace processor {

class Sink : public Operator {
public:
    Sink(Operator* prevOperator) : Operator(prevOperator), numTuples(0) {}
    void initialize(Graph* graph) { prevOperator->initialize(graph); }
    void getNextTuples();
    uint64_t getNumTuples() { return numTuples; }
    Operator* copy() { return new Sink(prevOperator->copy()); }

protected:
    uint64_t numTuples;
};

} // namespace processor
} // namespace graphflow
