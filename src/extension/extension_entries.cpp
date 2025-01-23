#include "common/string_utils.h"
#include "extension/extension_manager.h"

namespace kuzu::extension {

struct EntriesForExtension {
    const char* extensionName;
    std::span<const char* const> entries;
    size_t numEntries;
};

static constexpr std::array ftsExtensionFunctions = {"STEM", "QUERY_FTS_INDEX", "CREATE_FTS_INDEX",
    "DROP_FTS_INDEX"};
static constexpr std::array jsonExtensionFunctions = {"TO_JSON", "JSON_QUOTE", "ARRAY_TO_JSON",
    "ROW_TO_JSON", "CAST_TO_JSON", "JSON_ARRAY", "JSON_OBJECT", "JSON_MERGE_PATCH", "COPY_JSON",
    "JSON_EXTRACT", "JSON_ARRAY_LENGTH", "JSON_CONTAINS", "JSON_KEYS", "JSON_STRUCTURE",
    "JSON_TYPE", "JSON_VALID", "JSON"};
static constexpr std::array duckdbExtensionFunctions = {"CLEAR_ATTACHED_DB_CACHE"};
static constexpr std::array deltaExtensionFunctions = {"DELTA_SCAN"};
static constexpr std::array icebergExtensionFunctions = {"ICEBERG_SCAN", "ICEBERG_METADATA",
    "ICEBERG_SNAPSHOTS"};

static constexpr EntriesForExtension functionsForExtensionsRaw[] = {
    {"FTS", ftsExtensionFunctions, ftsExtensionFunctions.size()},
    {"FTS", ftsExtensionFunctions, ftsExtensionFunctions.size()},
    {"DUCKDB", duckdbExtensionFunctions, duckdbExtensionFunctions.size()},
    {"DELTA", deltaExtensionFunctions, deltaExtensionFunctions.size()},
    {"ICEBERG", icebergExtensionFunctions, ftsExtensionFunctions.size()},
    {"JSON", jsonExtensionFunctions, jsonExtensionFunctions.size()}};
static constexpr std::array functionsForExtensions = std::to_array(functionsForExtensionsRaw);

static constexpr std::array jsonExtensionTypes = {"JSON"};
static constexpr std::array<EntriesForExtension, 1> typesForExtensions = {
    EntriesForExtension{"JSON", jsonExtensionTypes, jsonExtensionTypes.size()}};

static std::optional<ExtensionEntry> lookupExtensionsByEntryName(std::string_view functionName,
    std::span<const EntriesForExtension> entriesForExtensions) {
    std::vector<ExtensionEntry> ret;
    for (const auto extension : entriesForExtensions) {
        for (const auto* entry : extension.entries) {
            if (entry == functionName) {
                return ExtensionEntry{.name = entry, .extensionName = extension.extensionName};
            }
        }
    }
    return {};
}

std::optional<ExtensionEntry> ExtensionManager::lookupExtensionsByFunctionName(
    std::string_view functionName) {
    return lookupExtensionsByEntryName(common::StringUtils::getUpper(functionName),
        functionsForExtensions);
}

std::optional<ExtensionEntry> ExtensionManager::lookupExtensionsByTypeName(
    std::string_view typeName) {
    return lookupExtensionsByEntryName(common::StringUtils::getUpper(typeName), typesForExtensions);
}
} // namespace kuzu::extension
