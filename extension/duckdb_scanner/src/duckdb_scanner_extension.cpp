#include "duckdb_scanner_extension.h"

#include "duckdb_scan.h"
#include "duckdb_storage.h"

namespace kuzu {
namespace duckdb_scanner {

void DuckDBScannerExtension::load(main::ClientContext* context) {
    auto db = context->getDatabase();
    db->registerStorageExtension("duckdb", std::make_unique<DuckDBStorageExtension>());
}

} // namespace duckdb_scanner
} // namespace kuzu

extern "C" {
// Because we link against the static library on windows, we implicitly inherit KUZU_STATIC_DEFINE,
// which cancels out any exporting, so we can't use KUZU_API.
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void init(kuzu::main::ClientContext* context) {
    kuzu::duckdb_scanner::DuckDBScannerExtension::load(context);
}
}
