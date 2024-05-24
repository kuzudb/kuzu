#include "processor/operator/standalone_call.h"

#include "common/cast.h"

namespace kuzu {
namespace processor {

bool StandaloneCall::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    if (standaloneCallInfo->hasExecuted) {
        return false;
    }
    standaloneCallInfo->hasExecuted = true;
    switch (standaloneCallInfo->option->optionType) {
    case main::OptionType::CONFIGURATION: {
        auto configurationOption =
            common::ku_dynamic_cast<main::Option*, main::ConfigurationOption*>(
                standaloneCallInfo->option);
        configurationOption->setContext(context->clientContext, standaloneCallInfo->optionValue);
        break;
    }
    case main::OptionType::EXTENSION:
        context->clientContext->setExtensionOption(standaloneCallInfo->option->name,
            standaloneCallInfo->optionValue);
        break;
    }
    metrics->numOutputTuple.increase(1);
    return true;
}

} // namespace processor
} // namespace kuzu
