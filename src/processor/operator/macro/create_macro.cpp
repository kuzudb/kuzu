#include "processor/operator/macro/create_macro.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool CreateMacro::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    if (hasExecuted) {
        return false;
    }
    createMacroInfo->catalog->addScalarMacroFunction(
        createMacroInfo->macroName, createMacroInfo->macro->copy());
    hasExecuted = true;
    outputVector->setValue<std::string>(outputVector->state->selVector->selectedPositions[0],
        StringUtils::string_format("Macro: {} has been created.", createMacroInfo->macroName));
    metrics->numOutputTuple.increase(1);
    return true;
}

} // namespace processor
} // namespace kuzu
