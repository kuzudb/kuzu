#include "json_extension.h"

#include "json_type.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace json_extension {

using namespace kuzu::extension;

void JsonExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    db.getCatalog()->createType(&transaction::DUMMY_TRANSACTION, JSON_TYPE_NAME,
        JsonType::getJsonType());
    addFuncsToCatalog(Functions{}, db);
}

} // namespace json_extension
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
    kuzu::json_extension::JsonExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::json_extension::JsonExtension::EXTENSION_NAME;
}
}
