#include "common/string_utils.h"
#include "extension/extension_manager.h"

namespace kuzu::extension {

static constexpr ExtensionEntry extensionFunctionsRaw[] = {{"STEM", "FTS"},
    {"QUERY_FTS_INDEX", "FTS"}, {"CREATE_FTS_INDEX", "FTS"}, {"DROP_FTS_INDEX", "FTS"},
    {"TO_JSON", "JSON"}, {"JSON_QUOTE", "JSON"}, {"ARRAY_TO_JSON", "JSON"}, {"ROW_TO_JSON", "JSON"},
    {"CAST_TO_JSON", "JSON"}, {"JSON_ARRAY", "JSON"}, {"JSON_OBJECT", "JSON"},
    {"JSON_MERGE_PATCH", "JSON"}, {"COPY_JSON", "JSON"}, {"JSON_EXTRACT", "JSON"},
    {"JSON_ARRAY_LENGTH", "JSON"}, {"JSON_CONTAINS", "JSON"}, {"JSON_KEYS", "JSON"},
    {"JSON_STRUCTURE", "JSON"}, {"JSON_TYPE", "JSON"}, {"JSON_VALID", "JSON"}, {"JSON", "JSON"},
    {"CLEAR_ATTACHED_DB_CACHE", "DUCKDB"}, {"DELTA_SCAN", "DELTA"}, {"ICEBERG_SCAN", "ICEBERG"},
    {"ICEBERG_METADATA", "ICEBERG"}, {"ICEBERG_SNAPSHOTS", "ICEBERG"}};
static constexpr std::array extensionFunctions = std::to_array(extensionFunctionsRaw);

static constexpr size_t NUM_EXTENSION_TYPES = 1;
static constexpr std::array<ExtensionEntry, NUM_EXTENSION_TYPES> extensionTypes = {
    {{"JSON", "JSON"}}};

static std::optional<ExtensionEntry> lookupExtensionsByEntryName(std::string_view functionName,
    std::span<const ExtensionEntry> entries) {
    std::vector<ExtensionEntry> ret;
    for (const auto& entry : entries) {
        if (entry.name == functionName) {
            return entry;
        }
    }
    return {};
}

std::optional<ExtensionEntry> ExtensionManager::lookupExtensionsByFunctionName(
    std::string_view functionName) {
    return lookupExtensionsByEntryName(common::StringUtils::getUpper(functionName),
        extensionFunctions);
}

std::optional<ExtensionEntry> ExtensionManager::lookupExtensionsByTypeName(
    std::string_view typeName) {
    return lookupExtensionsByEntryName(common::StringUtils::getUpper(typeName), extensionTypes);
}
} // namespace kuzu::extension
