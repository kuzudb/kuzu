#include "src/processor/include/plan/plan.h"

#include "src/processor/include/operator/operator_ser_deser_factory.h"

namespace graphflow {
namespace processor {

QueryPlan::QueryPlan(const string& fname) {
    FileDeserHelper fdsh{fname};
    lastOperator.reset(deserializeOperator(fdsh).release());
};

void QueryPlan::run() {
    while (lastOperator->hasNextMorsel()) {
        lastOperator->getNextTuples();
    }
}

void QueryPlan::serialize(const string& fname) {
    FileSerHelper fsh{fname};
    lastOperator->serialize(fsh);
}

} // namespace processor
} // namespace graphflow
