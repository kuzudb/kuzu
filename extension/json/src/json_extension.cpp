#include "json_extension.h"

#include "catalog/catalog.h"
#include "common/types/types.h"
#include "json_functions.h"
#include "json_scan.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace json_extension {

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
    if (!db.getCatalog()->containsType(&transaction::DUMMY_READ_TRANSACTION, "json")) {
        db.getCatalog()->createType(&transaction::DUMMY_WRITE_TRANSACTION, "json",
            common::LogicalType::STRING());
    }
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
