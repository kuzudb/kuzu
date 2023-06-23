#include "processor/operator/call.h"

namespace kuzu {
namespace processor {

bool Call::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    if (localState->hasExecuted) {
        return false;
    }
    localState->hasExecuted = true;
    localState->option.setContext(context->clientContext, localState->optionValue);
    metrics->numOutputTuple.increase(1);
    return true;
}

} // namespace processor
} // namespace kuzu
