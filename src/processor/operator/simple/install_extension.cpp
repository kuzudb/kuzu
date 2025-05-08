#include "processor/operator/simple/install_extension.h"

#include "common/string_format.h"
#include "processor/execution_context.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::extension;

void InstallExtension::executeInternal(ExecutionContext* context) {
    extension::ExtensionInstaller installer{info, *context->clientContext};
    bool installResult = installer.install();
    outputMessage =
        installResult ?
            common::stringFormat("Extension: {} has been installed from repo: {}.", info.name,
                info.repo) :
            common::stringFormat("Skip installing extension {} since it has already been "
                                 "installed.\nIf you want to update it, you can run: UPDATE {}.",
                info.name, info.name);
}

std::string InstallExtension::getOutputMsg() {
    return outputMessage;
}

} // namespace processor
} // namespace kuzu
