#include "src/processor/include/plan/plan.h"

namespace graphflow {
namespace processor {

void QueryPlan::run() {
    while (lastOperator->hasNextMorsel()) {
        lastOperator->getNextTuples();
    }
}

} // namespace processor
} // namespace graphflow
