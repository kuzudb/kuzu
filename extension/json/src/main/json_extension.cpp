#include "main/json_extension.h"

#include "json_cast_functions.h"
#include "json_creation_functions.h"
#include "json_export.h"
#include "json_extract_functions.h"
#include "json_scalar_functions.h"
#include "json_scan.h"
#include "json_type.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace json_extension {

using namespace kuzu::extension;

static void addJsonCreationFunction(transaction::Transaction* transaction, main::Database& db) {
    ExtensionUtils::addScalarFunc<ToJsonFunction>(transaction, db);
    ExtensionUtils::addScalarFuncAlias<JsonQuoteFunction>(transaction, db);
    ExtensionUtils::addScalarFuncAlias<ArrayToJsonFunction>(transaction, db);
    ExtensionUtils::addScalarFuncAlias<RowToJsonFunction>(transaction, db);
    ExtensionUtils::addScalarFunc<CastToJsonFunction>(transaction, db);
    ExtensionUtils::addScalarFunc<JsonArrayFunction>(transaction, db);
    ExtensionUtils::addScalarFunc<JsonObjectFunction>(transaction, db);
    ExtensionUtils::addScalarFunc<JsonMergePatchFunction>(transaction, db);
}

static void addJsonExtractFunction(transaction::Transaction* transaction, main::Database& db) {
    ExtensionUtils::addScalarFunc<JsonExtractFunction>(transaction, db);
}

static void addJsonScalarFunction(transaction::Transaction* transaction, main::Database& db) {
    ExtensionUtils::addScalarFunc<JsonArrayLengthFunction>(transaction, db);
    ExtensionUtils::addScalarFunc<JsonContainsFunction>(transaction, db);
    ExtensionUtils::addScalarFunc<JsonKeysFunction>(transaction, db);
    ExtensionUtils::addScalarFunc<JsonStructureFunction>(transaction, db);
    ExtensionUtils::addScalarFunc<JsonValidFunction>(transaction, db);
    ExtensionUtils::addScalarFunc<MinifyJsonFunction>(transaction, db);
}

void JsonExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    db.getCatalog()->createType(context->getTransaction(), JSON_TYPE_NAME, JsonType::getJsonType());
    addJsonCreationFunction(context->getTransaction(), db);
    addJsonExtractFunction(context->getTransaction(), db);
    addJsonScalarFunction(context->getTransaction(), db);
    ExtensionUtils::addScalarFunc<JsonExportFunction>(context->getTransaction(), db);
    ExtensionUtils::addTableFunc<JsonScan>(context->getTransaction(), db);
}

} // namespace json_extension
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
    kuzu::json_extension::JsonExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::json_extension::JsonExtension::EXTENSION_NAME;
}
}
#endif
