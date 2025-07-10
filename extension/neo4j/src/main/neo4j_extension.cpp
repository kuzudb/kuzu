#include "main/neo4j_extension.h"

#include "function/neo4j_migrate.h"
#include "main/client_context.h"

namespace kuzu {
namespace neo4j_extension {

using namespace extension;

void Neo4jExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    ExtensionUtils::addStandaloneTableFunc<Neo4jMigrateFunction>(db);
}

} // namespace neo4j_extension
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
    kuzu::neo4j_extension::Neo4jExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::neo4j_extension::Neo4jExtension::EXTENSION_NAME;
}
}
#endif
