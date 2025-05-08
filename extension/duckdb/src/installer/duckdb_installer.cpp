#include "installer/duckdb_installer.h"

#include "common/file_system/virtual_file_system.h"
#include "main/duckdb_extension.h"

namespace kuzu {
namespace duckdb_extension {

bool DuckDBInstaller::install() {
    auto loaderFileRepoInfo =
        extension::ExtensionUtils::getExtensionLoaderRepoInfo(info.name, info.repo);
    auto localLoaderFilePath =
        extension::ExtensionUtils::getLocalPathForExtensionLoader(&context, info.name);
    tryDownloadExtensionFile(loaderFileRepoInfo, localLoaderFilePath);

    for (auto& dependencyLib : DuckdbExtension::DEPENDENCY_LIB_FILES) {
        auto dependencyLibWithSuffix = extension::ExtensionUtils::appendLibSuffix(dependencyLib);
        auto localDependencyLibPath =
            extension::ExtensionUtils::getLocalPathForSharedLib(&context, dependencyLibWithSuffix);
        if (!context.getVFSUnsafe()->fileOrPathExists(localDependencyLibPath)) {
            auto dependencyLibRepoInfo =
                extension::ExtensionUtils::getSharedLibRepoInfo(dependencyLibWithSuffix, info.repo);
            tryDownloadExtensionFile(dependencyLibRepoInfo, localDependencyLibPath);
        }
    }
    return true;
}

} // namespace duckdb_extension
} // namespace kuzu
