#include "main/gds_extension.h"

#include "function/gds_function.h"
#include "main/client_context.h"

namespace kuzu {
namespace gds_extension {

using namespace extension;

void GdsExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    extension::ExtensionUtils::addTableFunc<SCCFunction>(db);
    extension::ExtensionUtils::addTableFunc<SCCKosarajuFunction>(db);
    extension::ExtensionUtils::addTableFunc<WeaklyConnectedComponentsFunction>(db);
    extension::ExtensionUtils::addTableFunc<PageRankFunction>(db);
    extension::ExtensionUtils::addTableFunc<KCoreDecompositionFunction>(db);
}

} // namespace gds_extension
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
    kuzu::gds_extension::GdsExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::gds_extension::GdsExtension::EXTENSION_NAME;
}
}
#endif
