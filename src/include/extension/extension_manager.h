#pragma once

#include "loaded_extension.h"
#include "storage/storage_extension.h"

namespace kuzu {
namespace main {}
namespace extension {

struct ExtensionEntry {
    const char* name;
    const char* extensionName;
};

class ExtensionManager {
public:
    static constexpr uint64_t EXTENSION_SIGNATURE_LEN = 256;

    static constexpr uint64_t SHA256_LEN = 32;

    static constexpr char PUBLIC_KEY[] =
        "-----BEGIN PUBLIC KEY-----\n"
        "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAhXoRMc6xWz1rFRd8vhbp\n"
        "0dFxfnqdY91Nhn1jbf7k/DhASFXuh2BIF5FgwtkXd2L1JbJHYS0PHTgKvolv+OMH\n"
        "yE217wMNGoeqbegwlMp5PIrUvmLCS+EIQ79zKMGg2tmQvZqj4rDNcYl9l26JShMM\n"
        "qOfGDTjXjUhfeWVADwq2+XE3QY/iwW/hUn2uiU/t+MjmNXRiqMR68BjQbTtbvz1R\n"
        "NWaWgdpq3q9jxeHCKIYGde8mqvGS5admQpL7my9NGnDRcz99E+12bB/PKPzeDi1l\n"
        "I2FnyXhNE1QoMk9jeoPVY84AqGBX8r8qhdeCGEogP/s6bwFCQcD/ce9lYoydxJIl\n"
        "lwIDAQAB\n"
        "-----END PUBLIC KEY-----";

public:
    void loadExtension(const std::string& path, main::ClientContext* context);

    KUZU_API std::string toCypher();

    KUZU_API void addExtensionOption(std::string name, common::LogicalTypeID type,
        common::Value defaultValue, bool isConfidential);

    const main::ExtensionOption* getExtensionOption(std::string name) const;

    KUZU_API void registerStorageExtension(std::string name,
        std::unique_ptr<storage::StorageExtension> storageExtension);

    std::vector<storage::StorageExtension*> getStorageExtensions();

    KUZU_API const std::vector<LoadedExtension>& getLoadedExtensions() const {
        return loadedExtensions;
    }

    static std::optional<ExtensionEntry> lookupExtensionsByFunctionName(
        std::string_view functionName);
    static std::optional<ExtensionEntry> lookupExtensionsByTypeName(std::string_view typeName);

    void autoLoadLinkedExtensions(main::ClientContext* context);

    KUZU_API static ExtensionManager* Get(const main::ClientContext& context);

private:
    std::vector<LoadedExtension> loadedExtensions;
    std::unordered_map<std::string, main::ExtensionOption> extensionOptions;
    common::case_insensitive_map_t<std::unique_ptr<storage::StorageExtension>> storageExtensions;
};

} // namespace extension
} // namespace kuzu
