#include "json_extension.h"

#include "catalog/catalog.h"
#include "common/types/types.h"
#include "json_creation_functions.h"
#include "json_export.h"
#include "json_extract_functions.h"
#include "json_scalar_functions.h"
#include "json_scan.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace json_extension {

static void addJsonCreationFunction(main::Database& db) {
    ADD_FUNC(ToJsonFunction);
    ADD_FUNC_ALIAS(JsonQuoteFunction);
    ADD_FUNC_ALIAS(ArrayToJsonFunction);
    ADD_FUNC_ALIAS(RowToJsonFunction);
    ADD_FUNC_ALIAS(CastToJsonFunction);
    ADD_FUNC(JsonArrayFunction);
    ADD_FUNC(JsonObjectFunction);
    ADD_FUNC(JsonMergePatchFunction);
}

static void addJsonExtractFunction(main::Database& db) {
    ADD_FUNC(JsonExtractFunction);
}

static void addJsonScalarFunction(main::Database& db) {
    ADD_FUNC(JsonArrayLengthFunction);
    ADD_FUNC(JsonContainsFunction);
    ADD_FUNC(JsonKeysFunction);
    ADD_FUNC(JsonStructureFunction);
    ADD_FUNC(JsonValidFunction);
    ADD_FUNC(MinifyJsonFunction);
}

void JsonExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    KU_ASSERT(!db.getCatalog()->containsType(context->getTx(), JSON_TYPE_NAME));
    db.getCatalog()->createType(context->getTx(), JSON_TYPE_NAME, common::LogicalType::STRING());
    addJsonCreationFunction(db);
    addJsonExtractFunction(db);
    addJsonScalarFunction(db);
    ADD_FUNC(JsonExportFunction);
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
