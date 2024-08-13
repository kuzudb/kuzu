#include "duckdb_loader.h"

#include "common/exception/io.h"
#include "common/file_system/virtual_file_system.h"
#include "common/system_message.h"
#include "extension/extension.h"
#include "main/client_context.h"
#ifdef _WIN32

#include "windows.h"
#define RTLD_NOW 0
#define RTLD_LOCAL 0

#else
#include <dlfcn.h>
#endif

namespace kuzu {
namespace duckdb {

void DuckDBLoader::load(main::ClientContext* context) {
w    for (auto& dependencyLib : DEPENDENCY_LIB_FILES) {
        auto dependencyLibPath =
            extension::ExtensionUtils::getLocalPathForSharedLib(context, dependencyLib);
        KU_ASSERT(context->getVFSUnsafe()->fileOrPathExists(dependencyLibPath));
        auto libHdl = dlopen(dependencyLibPath.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (libHdl == nullptr) {
            throw common::IOException(
                common::stringFormat("Lib \"{}\" could not be loaded.\nError: {}",
                    dependencyLibPath, common::dlErrMessage()));
        }
    }
}

} // namespace duckdb
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
    kuzu::duckdb::DuckDBLoader loader{};
    loader.load(context);
}
}
