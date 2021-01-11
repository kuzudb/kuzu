#include "src/processor/include/plan/physical/physical_plan.h"

#include <iostream>

namespace graphflow {
namespace processor {

void PhysicalPlan::run() {
    while (lastOperator->hasNextMorsel()) {
        lastOperator->getNextTuples();
    }
    lastOperator->cleanup();
}

} // namespace processor
} // namespace graphflow
