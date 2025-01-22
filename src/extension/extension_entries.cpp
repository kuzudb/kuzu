
#include "../../extension/fts/src/include/fts_extension.h"
#include "../../extension/json/src/include/json_extension.h"
#include "common/string_utils.h"
#include "common/type_utils.h"
#include "extension/extension_manager.h"
#include <span>

namespace kuzu::extension {

template<typename Extension>
struct GetExtensionFunctionEntries {
    template<typename Function>
    constexpr void operator()() {
        KU_ASSERT(numFilledEntries < entries.size());
        entries[numFilledEntries] = ExtensionFunctionEntry{.name = Function::name,
            .extensionName = Extension::EXTENSION_NAME};
        ++numFilledEntries;
    }

    std::span<ExtensionFunctionEntry> entries;
    uint64_t numFilledEntries = 0;
};

template<typename... Extensions>
constexpr decltype(auto) getExtensionFunctions() {
    constexpr size_t numFunctions =
        (std::tuple_size_v<typename Extensions::Functions::types> + ...);
    std::array<ExtensionFunctionEntry, numFunctions> ret{};
    size_t numFilledEntries = 0;
    (
        [&]() {
            auto helper = GetExtensionFunctionEntries<Extensions>{
                .entries = std::span{ret.begin() + numFilledEntries, ret.end()},
            };
            common::TypeUtils::forEachType(helper, typename Extensions::Functions{});
            numFilledEntries += helper.numFilledEntries;
        }(),
        ...);
    return ret;
}

static constexpr auto extensionFunctions =
    getExtensionFunctions<fts_extension::FTSExtension, json_extension::JsonExtension>();

std::optional<ExtensionFunctionEntry> ExtensionManager::lookupExtensionsByFunctionName(
    std::string_view functionName) {
    std::vector<ExtensionFunctionEntry> ret;
    for (const auto& entry : extensionFunctions) {
        if (common::StringUtils::getUpper(std::string_view{entry.name}) == functionName) {
            return entry;
        }
    }
    return {};
}
} // namespace kuzu::extension
