#pragma once

#include "loaded_extension.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "storage/storage_extension.h"

namespace kuzu {
namespace extension {

class ExtensionManager {
public:
    void loadExtension(const std::string& path, main::ClientContext* context);

    KUZU_API std::string toCypher();

    KUZU_API void addExtensionOption(std::string name, common::LogicalTypeID type,
        common::Value defaultValue);

    const main::ExtensionOption* getExtensionOption(std::string name) const;

    KUZU_API void registerStorageExtension(std::string name,
        std::unique_ptr<storage::StorageExtension> storageExtension);

    std::vector<storage::StorageExtension*> getStorageExtensions();

    KUZU_API const std::vector<LoadedExtension>& getLoadedExtensions() const {
        return loadedExtensions;
    }

private:
    std::vector<LoadedExtension> loadedExtensions;
    std::unordered_map<std::string, main::ExtensionOption> extensionOptions;
    common::case_insensitive_map_t<std::unique_ptr<storage::StorageExtension>> storageExtensions;
};

} // namespace extension
} // namespace kuzu
