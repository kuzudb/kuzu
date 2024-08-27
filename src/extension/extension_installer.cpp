#include "extension/extension_installer.h"

#include "common/exception/io.h"
#include "common/file_system/virtual_file_system.h"
#include "httplib.h"
#include "main/client_context.h"

namespace kuzu {
namespace extension {

void ExtensionInstaller::tryDownloadExtensionFile(main::ClientContext* context,
    const ExtensionRepoInfo& info, const std::string& localFilePath) {
    httplib::Client cli(info.hostURL.c_str());
    httplib::Headers headers = {
        {"User-Agent", common::stringFormat("kuzu/v{}", KUZU_EXTENSION_VERSION)}};
    auto res = cli.Get(info.hostPath.c_str(), headers);
    if (!res || res->status != 200) {
        if (res.error() == httplib::Error::Success) {
            // LCOV_EXCL_START
            throw common::IOException(common::stringFormat(
                "HTTP Returns: {}, Failed to download extension: \"{}\" from {}.",
                res.value().status, extensionName, info.repoURL));
            // LCOC_EXCL_STOP
        } else {
            throw common::IOException(
                common::stringFormat("Failed to download extension: {} at URL {} (ERROR: {})",
                    extensionName, info.repoURL, to_string(res.error())));
        }
    }

    auto vfs = context->getVFSUnsafe();
    auto fileInfo =
        vfs->openFile(localFilePath, common::FileFlags::WRITE | common::FileFlags::READ_ONLY |
                                         common::FileFlags::CREATE_AND_TRUNCATE_IF_EXISTS);
    fileInfo->writeFile(reinterpret_cast<const uint8_t*>(res->body.c_str()), res->body.size(),
        0 /* offset */);
    fileInfo->syncFile();
}

void ExtensionInstaller::install(main::ClientContext* context) {
    auto vfs = context->getVFSUnsafe();
    auto localExtensionDir = context->getExtensionDir();
    if (!vfs->fileOrPathExists(localExtensionDir, context)) {
        vfs->createDir(localExtensionDir);
    }
    auto localDirForExtension =
        extension::ExtensionUtils::getLocalExtensionDir(context, extensionName);
    if (!vfs->fileOrPathExists(localDirForExtension)) {
        vfs->createDir(localDirForExtension);
    }
    auto localDirForSharedLib = extension::ExtensionUtils::getLocalPathForSharedLib(context);
    if (!vfs->fileOrPathExists(localDirForSharedLib)) {
        vfs->createDir(localDirForSharedLib);
    }
    auto libFileRepoInfo = extension::ExtensionUtils::getExtensionLibRepoInfo(extensionName);
    auto localLibFilePath =
        extension::ExtensionUtils::getLocalPathForExtensionLib(context, extensionName);
    tryDownloadExtensionFile(context, libFileRepoInfo, localLibFilePath);
}

} // namespace extension
} // namespace kuzu
