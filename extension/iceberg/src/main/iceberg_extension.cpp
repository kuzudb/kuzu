#include "main/iceberg_extension.h"

#include "function/iceberg_functions.h"
#include "main/client_context.h"
#include "main/database.h"
#include "main/duckdb_extension.h"

namespace kuzu {
namespace iceberg_extension {

using namespace kuzu::extension;

void IcebergExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    ExtensionUtils::addTableFunc<IcebergScanFunction>(db);
    ExtensionUtils::addTableFunc<IcebergMetadataFunction>(db);
    ExtensionUtils::addTableFunc<IcebergSnapshotsFunction>(db);
    duckdb_extension::DuckdbExtension::loadRemoteFSOptions(context);
}

} // namespace iceberg_extension
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
    kuzu::iceberg_extension::IcebergExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::iceberg_extension::IcebergExtension::EXTENSION_NAME;
}
}
#endif
