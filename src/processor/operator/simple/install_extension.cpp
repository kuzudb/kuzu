#include "processor/operator/simple/install_extension.h"

#include "common/string_format.h"
#include "processor/execution_context.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::extension;

std::string InstallExtensionPrintInfo::toString() const {
    return "Install " + extensionName;
}

void InstallExtension::executeInternal(ExecutionContext* context) {
    installExtension(context->clientContext);
}

std::string InstallExtension::getOutputMsg() {
    return common::stringFormat("Extension: {} has been installed from repo: {}.", info.name,
        info.repo);
}

void InstallExtension::installExtension(main::ClientContext* context) {
    extension::ExtensionInstaller installer{info, *context};
    installer.install();
}

} // namespace processor
} // namespace kuzu
