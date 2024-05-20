#include "processor/operator/simple/install_extension.h"

#include "common/exception/io.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_format.h"
#include "extension/extension.h"
#include "httplib.h"
#include "main/database.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::extension;

void InstallExtension::executeInternal(kuzu::processor::ExecutionContext* context) {
    installExtension(context->clientContext);
}

std::string InstallExtension::getOutputMsg() {
    return common::stringFormat("Extension: {} has been installed.", name);
}

std::string InstallExtension::tryDownloadExtension() {
    auto extensionRepoInfo = ExtensionUtils::getExtensionRepoInfo(name);
    httplib::Client cli(extensionRepoInfo.hostURL.c_str());
    httplib::Headers headers = {
        {"User-Agent", common::stringFormat("kuzu/v{}", KUZU_EXTENSION_VERSION)}};
    auto res = cli.Get(extensionRepoInfo.hostPath.c_str(), headers);
    if (!res || res->status != 200) {
        if (res.error() == httplib::Error::Success) {
            // LCOV_EXCL_START
            throw IOException(common::stringFormat(
                "HTTP Returns: {}, Failed to download extension: \"{}\" from {}.",
                res.value().status, name, extensionRepoInfo.repoURL));
            // LCOC_EXCL_STOP
        } else {
            throw IOException(
                common::stringFormat("Failed to download extension: {} at URL {} (ERROR: {})", name,
                    extensionRepoInfo.repoURL, to_string(res.error())));
        }
    }
    return res->body;
}

void InstallExtension::saveExtensionToLocalFile(const std::string& extensionData,
    main::ClientContext* context) {
    auto extensionDir = context->getExtensionDir();
    auto extensionPath = ExtensionUtils::getExtensionPath(extensionDir, name);
    auto vfs = context->getVFSUnsafe();
    if (!vfs->fileOrPathExists(extensionDir, context)) {
        vfs->createDir(extensionDir);
    }
    auto fileInfo = vfs->openFile(extensionPath, O_WRONLY | O_CREAT);
    fileInfo->writeFile(reinterpret_cast<const uint8_t*>(extensionData.c_str()),
        extensionData.size(), 0 /* offset */);
    fileInfo->syncFile();
}

void InstallExtension::installExtension(main::ClientContext* context) {
    auto extensionData = tryDownloadExtension();
    saveExtensionToLocalFile(extensionData, context);
}

} // namespace processor
} // namespace kuzu
