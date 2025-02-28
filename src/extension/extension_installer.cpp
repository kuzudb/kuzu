#include "extension/extension_installer.h"

#include "common/exception/io.h"
#include "common/file_system/virtual_file_system.h"
#include "httplib.h"
#include "main/client_context.h"

namespace kuzu {
namespace extension {

void ExtensionInstaller::tryDownloadExtensionFile(const ExtensionRepoInfo& repoInfo,
    const std::string& localFilePath) {
    httplib::Client cli(repoInfo.hostURL.c_str());
    httplib::Headers headers = {
        {"User-Agent", common::stringFormat("kuzu/v{}", KUZU_EXTENSION_VERSION)}};
    auto res = cli.Get(repoInfo.hostPath.c_str(), headers);
    if (!res || res->status != 200) {
        if (res.error() == httplib::Error::Success) {
            // LCOV_EXCL_START
            throw common::IOException(common::stringFormat(
                "HTTP Returns: {}, Failed to download extension: \"{}\" from {}.",
                res.value().status, info.name, repoInfo.repoURL));
            // LCOC_EXCL_STOP
        } else {
            throw common::IOException(
                common::stringFormat("Failed to download extension: {} at URL {} (ERROR: {})",
                    info.name, repoInfo.repoURL, to_string(res.error())));
        }
    }

    auto vfs = context.getVFSUnsafe();
    auto fileInfo =
        vfs->openFile(localFilePath, common::FileFlags::WRITE | common::FileFlags::READ_ONLY |
                                         common::FileFlags::CREATE_AND_TRUNCATE_IF_EXISTS);
    fileInfo->writeFile(reinterpret_cast<const uint8_t*>(res->body.c_str()), res->body.size(),
        0 /* offset */);
    fileInfo->syncFile();
}

void ExtensionInstaller::install() {
    installExtension();
    installDependencies();
}

void ExtensionInstaller::installExtension() {
    auto vfs = context.getVFSUnsafe();
    auto localExtensionDir = context.getExtensionDir();
    if (!vfs->fileOrPathExists(localExtensionDir, &context)) {
        vfs->createDir(localExtensionDir);
    }
    auto localDirForExtension =
        extension::ExtensionUtils::getLocalExtensionDir(&context, info.name);
    if (!vfs->fileOrPathExists(localDirForExtension)) {
        vfs->createDir(localDirForExtension);
    }
    auto localDirForSharedLib = extension::ExtensionUtils::getLocalPathForSharedLib(&context);
    if (!vfs->fileOrPathExists(localDirForSharedLib)) {
        vfs->createDir(localDirForSharedLib);
    }
    auto libFileRepoInfo = extension::ExtensionUtils::getExtensionLibRepoInfo(info.name, info.repo);
    auto localLibFilePath =
        extension::ExtensionUtils::getLocalPathForExtensionLib(&context, info.name);
    tryDownloadExtensionFile(libFileRepoInfo, localLibFilePath);
}

void ExtensionInstaller::installDependencies() {
    auto extensionRepoInfo = ExtensionUtils::getExtensionInstallerRepoInfo(info.name, info.repo);
    httplib::Client cli(extensionRepoInfo.hostURL.c_str());
    httplib::Headers headers = {
        {"User-Agent", common::stringFormat("kuzu/v{}", KUZU_EXTENSION_VERSION)}};
    auto res = cli.Get(extensionRepoInfo.hostPath.c_str(), headers);
    if (!res || res->status != 200) {
        // The extension doesn't have an installer.
        return;
    }
    auto extensionInstallerPath =
        ExtensionUtils::getLocalPathForExtensionInstaller(&context, info.name);
    tryDownloadExtensionFile(extensionRepoInfo, extensionInstallerPath);
    auto libLoader = ExtensionLibLoader(info.name, extensionInstallerPath.c_str());
    auto install = libLoader.getInstallFunc();
    (*install)(info.repo, context);
}

} // namespace extension
} // namespace kuzu
