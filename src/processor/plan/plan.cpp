#include "src/processor/include/plan/plan.h"

#include "src/processor/include/operator/operator_ser_deser_factory.h"

namespace graphflow {
namespace processor {

QueryPlan::QueryPlan(FileDeserHelper& fdsh) : lastOperator{deserializeOperator(fdsh)} {};

void QueryPlan::run() {
    while (lastOperator->hasNextMorsel()) {
        lastOperator->getNextTuples();
    }
}

void QueryPlan::serialize(FileSerHelper& fsh) {
    lastOperator->serialize(fsh);
}

} // namespace processor
} // namespace graphflow
