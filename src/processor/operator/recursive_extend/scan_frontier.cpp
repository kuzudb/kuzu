#include "processor/operator/recursive_extend/scan_frontier.h"

namespace kuzu {
namespace processor {

bool ScanFrontier::getNextTuplesInternal(ExecutionContext* context) {
    if (!hasExecuted) {
        context->clientContext->progressBar->addJobsToPipeline(1);
        hasExecuted = true;
        context->clientContext->progressBar->finishJobsInPipeline(1);
        return true;
    }
    return false;
}

} // namespace processor
} // namespace kuzu
