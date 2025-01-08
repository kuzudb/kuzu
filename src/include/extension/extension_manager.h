#pragma once

#include "loaded_extension.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "storage/storage_extension.h"

namespace kuzu {
namespace extension {

class KUZU_API ExtensionManager {
public:
    void loadExtension(const std::string& path, main::ClientContext* context);

    std::string toCypher();

    void addExtensionOption(std::string name, common::LogicalTypeID type,
        common::Value defaultValue);

    const main::ExtensionOption* getExtensionOption(std::string name) const;

    void registerStorageExtension(std::string name,
        std::unique_ptr<storage::StorageExtension> storageExtension);

    common::case_insensitive_map_t<std::unique_ptr<storage::StorageExtension>>&
    getStorageExtensions();

private:
    std::vector<LoadedExtension> loadedExtensions;
    std::unordered_map<std::string, main::ExtensionOption> extensionOptions;
    common::case_insensitive_map_t<std::unique_ptr<storage::StorageExtension>> storageExtensions;
};

} // namespace extension
} // namespace kuzu
