#include "main/algo_extension.h"

#include "function/algo_function.h"
#include "main/client_context.h"

namespace kuzu {
namespace algo_extension {

using namespace extension;

void AlgoExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    extension::ExtensionUtils::addTableFunc<SCCFunction>(db);
    extension::ExtensionUtils::addTableFuncAlias<SCCAliasFunction>(db);
    extension::ExtensionUtils::addTableFunc<SCCKosarajuFunction>(db);
    extension::ExtensionUtils::addTableFuncAlias<SCCKosarajuAliasFunction>(db);
    extension::ExtensionUtils::addTableFunc<WeaklyConnectedComponentsFunction>(db);
    extension::ExtensionUtils::addTableFuncAlias<WeaklyConnectedComponentsAliasFunction>(db);
    extension::ExtensionUtils::addTableFunc<PageRankFunction>(db);
    extension::ExtensionUtils::addTableFuncAlias<PageRankAliasFunction>(db);
    extension::ExtensionUtils::addTableFunc<KCoreDecompositionFunction>(db);
    extension::ExtensionUtils::addTableFuncAlias<KCoreDecompositionAliasFunction>(db);
    extension::ExtensionUtils::addTableFunc<LouvainFunction>(db);
}

} // namespace algo_extension
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
    kuzu::algo_extension::AlgoExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::algo_extension::AlgoExtension::EXTENSION_NAME;
}
}
#endif
