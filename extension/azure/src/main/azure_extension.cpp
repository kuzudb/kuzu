#include "main/azure_extension.h"

#include "function/azure_scan.h"
#include "main/client_context.h"
#include "main/database.h"
#include "main/duckdb_extension.h"

namespace kuzu {
namespace azure_extension {

void AzureExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    extension::ExtensionUtils::addTableFunc<AzureScanFunction>(db);
    duckdb_extension::DuckdbExtension::loadRemoteFSOptions(context);
}

} // namespace azure_extension
} // namespace kuzu

#if defined(BUILD_DYNAMIC_LOAD)
extern "C" {
// Because we link against the static library on windows, we implicitly inherit KUZU_STATIC_DEFINE,
// which cancels out any exporting, so we can't use KUZU_API.
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void init(kuzu::main::ClientContext* context) {
    kuzu::azure_extension::AzureExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::azure_extension::AzureExtension::EXTENSION_NAME;
}
}
#endif
