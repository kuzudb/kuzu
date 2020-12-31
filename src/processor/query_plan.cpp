#include "src/processor/include/query_plan.h"

namespace graphflow {
namespace processor {

void QueryPlan::run() {
    while (lastOperator->hasNextMorsel()) {
        lastOperator->getNextTuples();
    }
}

} // namespace processor
} // namespace graphflow
