#include "main/algo_extension.h"

#include "function/algo_function.h"
#include "main/client_context.h"

namespace kuzu {
namespace algo_extension {

using namespace extension;

void AlgoExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    ExtensionUtils::addTableFunc<SCCFunction>(db);
    ExtensionUtils::addTableFuncAlias<SCCAliasFunction>(db);
    ExtensionUtils::addTableFunc<SCCKosarajuFunction>(db);
    ExtensionUtils::addTableFuncAlias<SCCKosarajuAliasFunction>(db);
    ExtensionUtils::addTableFunc<WeaklyConnectedComponentsFunction>(db);
    ExtensionUtils::addTableFuncAlias<WeaklyConnectedComponentsAliasFunction>(db);
    ExtensionUtils::addTableFunc<PageRankFunction>(db);
    ExtensionUtils::addTableFuncAlias<PageRankAliasFunction>(db);
    ExtensionUtils::addTableFunc<KCoreDecompositionFunction>(db);
    ExtensionUtils::addTableFuncAlias<KCoreDecompositionAliasFunction>(db);
    ExtensionUtils::addTableFunc<LouvainFunction>(db);
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
