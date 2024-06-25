#include "json_extension.h"

#include "json_functions.h"
#include "json_scan.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace json_extension {

#define ADD_FUNC(FUNC_STRUCT) \
extension::ExtensionUtils::registerFunctionSet(db, std::string(FUNC_STRUCT::name), FUNC_STRUCT::getFunctionSet())

void JsonExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    ADD_FUNC(ToJsonFunction);
    ADD_FUNC(JsonMergeFunction);
    ADD_FUNC(JsonExtractFunction);
    ADD_FUNC(JsonArrayLengthFunction);
    ADD_FUNC(JsonContainsFunction);
    ADD_FUNC(JsonKeysFunction);
    ADD_FUNC(JsonStructureFunction);
    ADD_FUNC(JsonValidFunction);
    ADD_FUNC(MinifyJsonFunction);
    extension::ExtensionUtils::registerTableFunction(db, JsonScan::getFunction());
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
}
