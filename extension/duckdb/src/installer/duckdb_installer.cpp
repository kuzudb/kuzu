#include "installer/duckdb_installer.h"

#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"
#include "main/duckdb_extension.h"

namespace kuzu {
namespace duckdb_extension {

void DuckDBInstaller::install(main::ClientContext* context) {
    auto loaderFileRepoInfo = extension::ExtensionUtils::getExtensionLoaderRepoInfo(extensionName);
    auto localLoaderFilePath =
        extension::ExtensionUtils::getLocalPathForExtensionLoader(context, extensionName);
    tryDownloadExtensionFile(context, loaderFileRepoInfo, localLoaderFilePath);

    for (auto& dependencyLib : DuckDBExtension::DEPENDENCY_LIB_FILES) {
        auto dependencyLibWithSuffix = extension::ExtensionUtils::appendLibSuffix(dependencyLib);
        auto localDependencyLibPath =
            extension::ExtensionUtils::getLocalPathForSharedLib(context, dependencyLibWithSuffix);
        if (!context->getVFSUnsafe()->fileOrPathExists(localDependencyLibPath)) {
            auto dependencyLibRepoInfo =
                extension::ExtensionUtils::getSharedLibRepoInfo(dependencyLibWithSuffix);
            tryDownloadExtensionFile(context, dependencyLibRepoInfo, localDependencyLibPath);
        }
    }
}

} // namespace duckdb_extension
} // namespace kuzu
