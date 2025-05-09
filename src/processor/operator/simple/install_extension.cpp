#include "processor/operator/simple/install_extension.h"

#include "common/string_format.h"
#include "processor/execution_context.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::extension;

void InstallExtension::setOutputMessage(bool installed) {
    if (info.forceInstall) {
        outputMessage =
            common::stringFormat("Extension: {} updated from the repo: {}.", info.name, info.repo);
        return;
    }
    if (installed) {
        outputMessage = common::stringFormat("Extension: {} installed from the repo: {}.",
            info.name, info.repo);
    } else {
        outputMessage = common::stringFormat(
            "Extension: {} is already installed.\nTo update it, you can run: UPDATE {}.", info.name,
            info.name);
    }
}

void InstallExtension::executeInternal(ExecutionContext* context) {
    extension::ExtensionInstaller installer{info, *context->clientContext};
    bool installResult = installer.install();
    setOutputMessage(installResult);
    if (info.forceInstall) {
        KU_ASSERT(installResult);
    }
}

std::string InstallExtension::getOutputMsg() {
    return outputMessage;
}

} // namespace processor
} // namespace kuzu
