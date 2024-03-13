#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

void DDL::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    outputVector = resultSet->getValueVector(outputPos).get();
}

bool DDL::getNextTuplesInternal(ExecutionContext* context) {
    if (hasExecuted) {
        return false;
    }
    context->clientContext->progressBar->addJobsToPipeline(1);
    hasExecuted = true;
    executeDDLInternal(context);
    outputVector->setValue<std::string>(0, getOutputMsg());
    metrics->numOutputTuple.increase(1);
    context->clientContext->progressBar->finishJobsInPipeline(1);
    return true;
}

} // namespace processor
} // namespace kuzu
