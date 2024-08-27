#include "processor/operator/simple/install_extension.h"

#include "common/file_system/virtual_file_system.h"
#include "common/string_format.h"
#include "extension/extension.h"
#include "extension/extension_installer.h"
#include "httplib.h"
#include "main/database.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::extension;

std::string InstallExtensionPrintInfo::toString() const {
    return "Install " + extensionName;
}

void InstallExtension::executeInternal(kuzu::processor::ExecutionContext* context) {
    installExtension(context->clientContext);
}

std::string InstallExtension::getOutputMsg() {
    return common::stringFormat("Extension: {} has been installed.", name);
}

static void saveExtensionToLocalFile(const std::string& extensionData, const std::string& name,
    main::ClientContext* context) {
    auto extensionPath = ExtensionUtils::getLocalPathForExtensionInstaller(context, name);
    auto fileInfo = context->getVFSUnsafe()->openFile(extensionPath,
        FileFlags::WRITE | FileFlags::CREATE_AND_TRUNCATE_IF_EXISTS);
    fileInfo->writeFile(reinterpret_cast<const uint8_t*>(extensionData.c_str()),
        extensionData.size(), 0 /* offset */);
    fileInfo->syncFile();
}

static void installDependencies(const std::string& name, main::ClientContext* context) {
    auto extensionRepoInfo = ExtensionUtils::getExtensionInstallerRepoInfo(name);
    httplib::Client cli(extensionRepoInfo.hostURL.c_str());
    httplib::Headers headers = {
        {"User-Agent", common::stringFormat("kuzu/v{}", KUZU_EXTENSION_VERSION)}};
    auto res = cli.Get(extensionRepoInfo.hostPath.c_str(), headers);
    if (!res || res->status != 200) {
        return;
    }
    saveExtensionToLocalFile(res->body, name, context);
    auto extensionInstallerPath = ExtensionUtils::getLocalPathForExtensionInstaller(context, name);
    auto libLoader = ExtensionLibLoader(name, extensionInstallerPath.c_str());
    auto install = libLoader.getInstallFunc();
    (*install)(context);
}

void InstallExtension::installExtension(main::ClientContext* context) {
    extension::ExtensionInstaller installer{name};
    installer.install(context);
    installDependencies(name, context);
}

} // namespace processor
} // namespace kuzu
