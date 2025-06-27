#include "main/algo_extension.h"

#include "function/algo_function.h"
#include "main/client_context.h"

namespace kuzu {
namespace algo_extension {

using namespace extension;

void AlgoExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    ExtensionUtils::addTableFunc<SCCFunction>(context->getTransaction(), db);
    ExtensionUtils::addTableFuncAlias<SCCAliasFunction>(context->getTransaction(), db);
    ExtensionUtils::addTableFunc<SCCKosarajuFunction>(context->getTransaction(), db);
    ExtensionUtils::addTableFuncAlias<SCCKosarajuAliasFunction>(context->getTransaction(), db);
    ExtensionUtils::addTableFunc<WeaklyConnectedComponentsFunction>(context->getTransaction(), db);
    ExtensionUtils::addTableFuncAlias<WeaklyConnectedComponentsAliasFunction>(
        context->getTransaction(), db);
    ExtensionUtils::addTableFunc<PageRankFunction>(context->getTransaction(), db);
    ExtensionUtils::addTableFuncAlias<PageRankAliasFunction>(context->getTransaction(), db);
    ExtensionUtils::addTableFunc<KCoreDecompositionFunction>(context->getTransaction(), db);
    ExtensionUtils::addTableFuncAlias<KCoreDecompositionAliasFunction>(context->getTransaction(),
        db);
    ExtensionUtils::addTableFunc<LouvainFunction>(context->getTransaction(), db);
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
