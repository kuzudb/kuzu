#pragma once

#include "extension/extension.h"
#include "json_creation_functions.h"
#include "json_export.h"
#include "json_extract_functions.h"
#include "json_scalar_functions.h"
#include "json_scan.h"

namespace kuzu {
namespace json_extension {

class JsonExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "JSON";
    using Functions = common::TypeList<ToJsonFunction, JsonQuoteFunction, ArrayToJsonFunction,
        RowToJsonFunction, CastToJsonFunction, JsonArrayFunction, JsonObjectFunction,
        JsonMergePatchFunction, JsonExtractFunction, JsonArrayLengthFunction, JsonContainsFunction,
        JsonKeysFunction, JsonStructureFunction, JsonValidFunction, MinifyJsonFunction, JsonScan,
        JsonExportFunction>;

public:
    static constexpr char JSON_TYPE_NAME[] = "json";
    static constexpr common::idx_t JSON_SCAN_FILE_IDX = 0;

    static void load(main::ClientContext* context);
};

} // namespace json_extension
} // namespace kuzu
