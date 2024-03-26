#include "postgres_scanner_extension.h"

#include "postgres_storage.h"

namespace kuzu {
namespace postgres_scanner {

void PostgresScannerExtension::load(main::ClientContext* context) {
    auto db = context->getDatabase();
    db->registerStorageExtension("postgres", std::make_unique<PostgresStorageExtension>());
}

} // namespace postgres_scanner
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
    kuzu::postgres_scanner::PostgresScannerExtension::load(context);
}
}
