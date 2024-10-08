#include "processor/operator/standalone_call.h"

#include "common/cast.h"

namespace kuzu {
namespace processor {

std::string StandaloneCallPrintInfo::toString() const {
    std::string result = "Function: ";
    result += functionName;
    return result;
}

bool StandaloneCall::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    if (standaloneCallInfo->hasExecuted) {
        return false;
    }
    standaloneCallInfo->hasExecuted = true;
    switch (standaloneCallInfo->option->optionType) {
    case main::OptionType::CONFIGURATION: {
        auto configurationOption =
            common::ku_dynamic_cast<main::ConfigurationOption*>(standaloneCallInfo->option);
        configurationOption->setContext(context->clientContext, standaloneCallInfo->optionValue);
        break;
    }
    case main::OptionType::EXTENSION:
        context->clientContext->setExtensionOption(standaloneCallInfo->option->name,
            standaloneCallInfo->optionValue);
        break;
    }
    metrics->numOutputTuple.incrementByOne();
    return true;
}

} // namespace processor
} // namespace kuzu
