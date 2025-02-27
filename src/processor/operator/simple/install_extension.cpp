#include "processor/operator/simple/install_extension.h"

#include "common/file_system/virtual_file_system.h"
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

// static void saveExtensionToLocalFile(const std::string& extensionData, const std::string& name,
//     main::ClientContext* context) {
//     auto extensionPath = ExtensionUtils::getLocalPathForExtensionInstaller(context, name);
//     auto fileInfo = context->getVFSUnsafe()->openFile(extensionPath,
//         FileFlags::WRITE | FileFlags::CREATE_AND_TRUNCATE_IF_EXISTS);
//     fileInfo->writeFile(reinterpret_cast<const uint8_t*>(extensionData.c_str()),
//         extensionData.size(), 0 /* offset */);
//     fileInfo->syncFile();
// }

void InstallExtension::installExtension(main::ClientContext* context) {
    extension::ExtensionInstaller installer{info, *context};
    installer.install();
}

} // namespace processor
} // namespace kuzu
