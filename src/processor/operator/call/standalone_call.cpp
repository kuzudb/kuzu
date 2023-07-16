#include "processor/operator/call/standalone_call.h"

namespace kuzu {
namespace processor {

bool StandaloneCall::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    if (standaloneCallInfo->hasExecuted) {
        return false;
    }
    standaloneCallInfo->hasExecuted = true;
    standaloneCallInfo->option.setContext(context->clientContext, standaloneCallInfo->optionValue);
    metrics->numOutputTuple.increase(1);
    return true;
}

} // namespace processor
} // namespace kuzu
