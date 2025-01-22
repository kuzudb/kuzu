#include "extension/extension_manager.h"

namespace kuzu::extension {

static constexpr size_t NUM_EXTENSION_FUNCTIONS = 50;
static constexpr std::array<ExtensionFunctionEntry, NUM_EXTENSION_FUNCTIONS> extensionFunctions = {
    {{"STEM", "FTS"}, {"QUERY_FTS_INDEX", "FTS"}, {"CREATE_FTS_INDEX", "FTS"},
        {"DROP_FTS_INDEX", "FTS"}, {"TO_JSON", "JSON"}, {"JSON_QUOTE", "JSON"},
        {"ARRAY_TO_JSON", "JSON"}, {"ROW_TO_JSON", "JSON"}, {"CAST_TO_JSON", "JSON"},
        {"JSON_ARRAY", "JSON"}, {"JSON_OBJECT", "JSON"}, {"JSON_MERGE_PATCH", "JSON"},
        {"COPY_JSON", "JSON"}, {"JSON_EXTRACT", "JSON"}, {"JSON_ARRAY_LENGTH", "JSON"},
        {"JSON_CONTAINS", "JSON"}, {"JSON_KEYS", "JSON"}, {"JSON_STRUCTURE", "JSON"},
        {"JSON_TYPE", "JSON"}, {"JSON_VALID", "JSON"}, {"JSON", "JSON"},
        {"CLEAR_ATTACHED_DB_CACHE", "DUCKDB"}, {"DELTA_SCAN", "DELTA"}, {"ICEBERG_SCAN", "ICEBERG"},
        {"ICEBERG_METADATA", "ICEBERG"}, {"ICEBERG_SNAPSHOTS", "ICEBERG"}}};

std::optional<ExtensionFunctionEntry> ExtensionManager::lookupExtensionsByFunctionName(
    std::string_view functionName) {
    std::vector<ExtensionFunctionEntry> ret;
    for (const auto& entry : extensionFunctions) {
        if (entry.name == functionName) {
            return entry;
        }
    }
    return {};
}
} // namespace kuzu::extension
