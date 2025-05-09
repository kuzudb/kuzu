#include "processor/operator/simple/install_extension.h"

#include "common/string_format.h"
#include "processor/execution_context.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::extension;

void InstallExtension::executeInternal(ExecutionContext* context) {
    extension::ExtensionInstaller installer{info, *context->clientContext};
    installer.install();
}

std::string InstallExtension::getOutputMsg() {
    return common::stringFormat("Extension: {} has been installed from repo: {}.", info.name,
        info.repo);
}

} // namespace processor
} // namespace kuzu
