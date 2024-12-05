#include "loader/duckdb_loader.h"

#include "main/duckdb_extension.h"

namespace kuzu {
namespace duckdb_extension {

void DuckDBLoader::loadDependency(main::ClientContext* context) {
    for (auto& dependencyLib : DuckDBExtension::DEPENDENCY_LIB_FILES) {
        auto dependencyLibWithSuffix = extension::ExtensionUtils::appendLibSuffix(dependencyLib);
        auto dependencyLibPath =
            extension::ExtensionUtils::getLocalPathForSharedLib(context, dependencyLibWithSuffix);
        auto dependencyLoader = extension::ExtensionLibLoader(extensionName, dependencyLibPath);
    }
}

} // namespace duckdb_extension
} // namespace kuzu

extern "C" {
// Because we link against the static library on windows, we implicitly inherit KUZU_STATIC_DEFINE,
// which cancels out any exporting, so we can't use KUZU_API.
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void load(kuzu::main::ClientContext* context) {
    kuzu::duckdb_extension::DuckDBLoader loader{"duckdb"};
    loader.loadDependency(context);
}
}
