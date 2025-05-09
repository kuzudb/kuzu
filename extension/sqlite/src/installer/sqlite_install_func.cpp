#include "installer/duckdb_installer.h"

extern "C" {
// Because we link against the static library on windows, we implicitly inherit KUZU_STATIC_DEFINE,
// which cancels out any exporting, so we can't use KUZU_API.
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void install(const std::string& repo, kuzu::main::ClientContext& context) {
    kuzu::extension::InstallExtensionInfo info{"sqlite", repo, false /* forceInstall */};
    kuzu::duckdb_extension::DuckDBInstaller installer{info, context};
    installer.install();
}
}
