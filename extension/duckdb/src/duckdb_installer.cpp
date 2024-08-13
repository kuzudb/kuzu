#include "duckdb_installer.h"

#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"

namespace kuzu {
namespace duckdb {

void DuckDBInstaller::install(main::ClientContext* context) {
    auto loaderFileRepoInfo = extension::ExtensionUtils::getExtensionLoaderRepoInfo(extensionName);
    auto localLoaderFilePath =
        extension::ExtensionUtils::getLocalPathForExtensionLoader(context, extensionName);
    tryDownloadExtensionFile(context, loaderFileRepoInfo, localLoaderFilePath);

    for (auto& dependencyLib : DEPENDENCY_LIB_FILES) {
        auto localDependencyLibPath =
            extension::ExtensionUtils::getLocalPathForSharedLib(context, dependencyLib);
        if (!context->getVFSUnsafe()->fileOrPathExists(localDependencyLibPath)) {
            auto dependencyLibRepoInfo =
                extension::ExtensionUtils::getSharedLibRepoInfo(dependencyLib);
            tryDownloadExtensionFile(context, dependencyLibRepoInfo, localDependencyLibPath);
        }
    }
}

} // namespace duckdb
} // namespace kuzu
