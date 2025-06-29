#include "processor/operator/simple/load_extension.h"

#include "extension/extension_manager.h"
#include "processor/execution_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

using namespace kuzu::extension;

std::string LoadExtensionPrintInfo::toString() const {
    return "Load " + extensionName;
}

void LoadExtension::executeInternal(ExecutionContext* context) {
    auto clientContext = context->clientContext;
    clientContext->getExtensionManager()->loadExtension(path, clientContext);
    appendMessage(stringFormat("Extension: {} has been loaded.", path),
        clientContext->getMemoryManager());
}

} // namespace processor
} // namespace kuzu
