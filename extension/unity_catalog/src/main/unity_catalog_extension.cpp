
#include "main/unity_catalog_extension.h"

#include "main/client_context.h"
#include "options/unity_catalog_options.h"
#include "storage/unity_catalog_storage.h"

namespace kuzu {
namespace unity_catalog_extension {

void UnityCatalogExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    db.registerStorageExtension(EXTENSION_NAME, std::make_unique<UnityCatalogStorageExtension>(db));
    UnityCatalogOptions::registerExtensionOptions(&db);
    UnityCatalogOptions::setEnvValue(context);
}

} // namespace unity_catalog_extension
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
    kuzu::unity_catalog_extension::UnityCatalogExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::unity_catalog_extension::UnityCatalogExtension::EXTENSION_NAME;
}
}
#endif
