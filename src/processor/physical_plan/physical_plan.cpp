#include "src/processor/include/physical_plan/physical_plan.h"

namespace graphflow {
namespace processor {

void PhysicalPlan::run() {
    while (lastOperator->hasNextMorsel()) {
        lastOperator->getNextTuples();
    }
}

} // namespace processor
} // namespace graphflow
