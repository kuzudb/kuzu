#include "processor/operator/simple/load_extension.h"

#include "extension/extension_manager.h"
#include "main/database.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

using namespace kuzu::extension;

std::string LoadExtensionPrintInfo::toString() const {
    return "Load " + extensionName;
}

void LoadExtension::executeInternal(kuzu::processor::ExecutionContext* context) {
    context->clientContext->getExtensionManager()->loadExtension(path, context->clientContext);
}

std::string LoadExtension::getOutputMsg() {
    return stringFormat("Extension: {} has been loaded.", path);
}

} // namespace processor
} // namespace kuzu
