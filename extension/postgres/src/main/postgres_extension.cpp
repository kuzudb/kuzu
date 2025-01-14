#include "main/postgres_extension.h"

#include "main/client_context.h"
#include "main/database.h"
#include "storage/postgres_storage.h"

namespace kuzu {
namespace postgres_extension {

void PostgresExtension::load(main::ClientContext* context) {
    auto db = context->getDatabase();
    db->registerStorageExtension(EXTENSION_NAME, std::make_unique<PostgresStorageExtension>(db));
}

} // namespace postgres_extension
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
    kuzu::postgres_extension::PostgresExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::postgres_extension::PostgresExtension::EXTENSION_NAME;
}
}
