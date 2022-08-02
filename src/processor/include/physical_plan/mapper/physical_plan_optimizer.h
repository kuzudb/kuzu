#pragma once

#include "src/processor/include/physical_plan/physical_plan.h"

namespace graphflow {
namespace processor {

class SemiJoinOptimizer {
public:
    static void tryOptimize(PhysicalPlan* plan) { tryOptimize(plan->lastOperator.get()); }

private:
    static void tryOptimize(PhysicalOperator* op);
    static void optimize(PhysicalOperator* op);

    static vector<PhysicalOperator*> collectScanNodeID(PhysicalOperator* op);
    static void collectScanNodeIDRecursive(
        PhysicalOperator* op, vector<PhysicalOperator*>& scanNodeIDOps);
};

} // namespace processor
} // namespace graphflow
