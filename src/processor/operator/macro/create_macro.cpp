#include "processor/operator/macro/create_macro.h"

#include "common/string_format.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string CreateMacroPrintInfo::toString() const {
    return macroName;
}

void CreateMacro::executeInternal(ExecutionContext* context) {
    auto clientContext = context->clientContext;
    auto catalog = catalog::Catalog::Get(*clientContext);
    auto transaction = clientContext->getTransaction();
    catalog->addScalarMacroFunction(transaction, info.macroName, info.macro->copy());
    appendMessage(stringFormat("Macro: {} has been created.", info.macroName),
        storage::MemoryManager::Get(*clientContext));
}

} // namespace processor
} // namespace kuzu
