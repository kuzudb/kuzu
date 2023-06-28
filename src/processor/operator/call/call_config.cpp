#include "processor/operator/call/call_config.h"

namespace kuzu {
namespace processor {

bool CallConfig::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    if (callConfigInfo->hasExecuted) {
        return false;
    }
    callConfigInfo->hasExecuted = true;
    callConfigInfo->option.setContext(context->clientContext, callConfigInfo->optionValue);
    metrics->numOutputTuple.increase(1);
    return true;
}

} // namespace processor
} // namespace kuzu
