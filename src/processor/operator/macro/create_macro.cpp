#include "processor/operator/macro/create_macro.h"

#include "common/string_format.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string CreateMacroPrintInfo::toString() const {
    return macroName;
}

bool CreateMacro::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    if (hasExecuted) {
        return false;
    }
    createMacroInfo->catalog->addScalarMacroFunction(context->clientContext->getTx(),
        createMacroInfo->macroName, createMacroInfo->macro->copy());
    hasExecuted = true;
    outputVector->setValue<std::string>(outputVector->state->getSelVector()[0],
        stringFormat("Macro: {} has been created.", createMacroInfo->macroName));
    metrics->numOutputTuple.incrementByOne();
    return true;
}

} // namespace processor
} // namespace kuzu
