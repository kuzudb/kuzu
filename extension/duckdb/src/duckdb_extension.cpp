#include "duckdb_extension.h"

#include "duckdb_storage.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace duckdb_extension {

void DuckDBExtension::load(main::ClientContext* context) {
    auto db = context->getDatabase();
    db->registerStorageExtension(EXTENSION_NAME, std::make_unique<DuckDBStorageExtension>(db));
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
INIT_EXPORT void init(kuzu::main::ClientContext* context) {
    kuzu::duckdb_extension::DuckDBExtension::load(context);
}
}
