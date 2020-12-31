#pragma once

#include <memory>
#include <vector>

#include "src/processor/include/operator/operator.h"
#include "src/processor/include/operator/sink/sink.h"

namespace graphflow {
namespace processor {

class QueryPlan {

public:
    QueryPlan(Operator* lastOperator) : lastOperator(unique_ptr<Operator>(lastOperator)) {}
    QueryPlan(const QueryPlan& plan) : lastOperator() {
        this->lastOperator = unique_ptr<Operator>(plan.lastOperator->copy());
    }

    inline void initialize(Graph* graph) { lastOperator->initialize(graph); }

    void run();

    uint64_t getNumTuples() { return ((Sink*)lastOperator.get())->getNumTuples(); }

private:
    unique_ptr<Operator> lastOperator;
};

} // namespace processor
} // namespace graphflow
