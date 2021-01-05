#pragma once

#include <memory>
#include <vector>

#include "src/common/include/file_ser_deser_helper.h"
#include "src/processor/include/operator/operator.h"
#include "src/processor/include/operator/sink/sink.h"
#include "src/processor/include/plan/plan_output.h"

namespace graphflow {
namespace processor {

class QueryPlan {

public:
    QueryPlan(unique_ptr<Operator> lastOperator) : lastOperator{move(lastOperator)} {}
    QueryPlan(FileDeserHelper& fdsh);

    QueryPlan(const QueryPlan& plan)
        : lastOperator{unique_ptr<Operator>(plan.lastOperator->clone())} {};

    inline void initialize(Graph* graph) { lastOperator->initialize(graph); }

    void run();

    unique_ptr<PlanOutput> getPlanOutput() {
        return make_unique<PlanOutput>(((Sink*)lastOperator.get())->getNumTuples());
    }

    void serialize(FileSerHelper& fsh);

private:
    unique_ptr<Operator> lastOperator;
};

} // namespace processor
} // namespace graphflow
