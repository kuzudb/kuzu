#include "processor/operator/macro/create_macro.h"

#include "common/string_format.h"
#include "processor/execution_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string CreateMacroPrintInfo::toString() const {
    return macroName;
}

void CreateMacro::executeInternal(ExecutionContext* context) {
    auto clientContext = context->clientContext;
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTransaction();
    catalog->addScalarMacroFunction(transaction, info.macroName, info.macro->copy());
    appendMessage(stringFormat("Macro: {} has been created.", info.macroName),
        clientContext->getMemoryManager());
}

} // namespace processor
} // namespace kuzu
