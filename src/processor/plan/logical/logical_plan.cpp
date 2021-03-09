#include "src/processor/include/plan/logical/logical_plan.h"

#include "src/processor/include/operator/logical/logical_operator_ser_deser.h"

namespace graphflow {
namespace processor {

LogicalPlan::LogicalPlan(const string& path) {
    FileDeserHelper fdsh{path};
    lastOperator.reset(deserializeOperator(fdsh).release());
}

unique_ptr<PhysicalPlan> LogicalPlan::mapToPhysical(const Graph& graph) {
    auto physicalOperatorInfo = PhysicalOperatorsInfo();
    auto sink = make_unique<Sink>(lastOperator->mapToPhysical(graph, physicalOperatorInfo));
    return make_unique<PhysicalPlan>(move(sink));
};

void LogicalPlan::serialize(const string& fname) {
    FileSerHelper fsh{fname};
    lastOperator->serialize(fsh);
}

} // namespace processor
} // namespace graphflow
