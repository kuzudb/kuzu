#include "extension/extension_manager.h"

#include "common/file_system/virtual_file_system.h"
#include "common/string_utils.h"
#include "extension/extension.h"
#include "generated_extension_loader.h"

namespace kuzu {
namespace extension {

static void executeExtensionLoader(main::ClientContext* context, const std::string& extensionName) {
    auto loaderPath = ExtensionUtils::getLocalPathForExtensionLoader(context, extensionName);
    if (context->getVFSUnsafe()->fileOrPathExists(loaderPath)) {
        auto libLoader = ExtensionLibLoader(extensionName, loaderPath);
        auto load = libLoader.getLoadFunc();
        (*load)(context);
    }
}

void ExtensionManager::loadExtension(const std::string& path, main::ClientContext* context) {
    auto fullPath = path;
    bool isOfficial = extension::ExtensionUtils::isOfficialExtension(path);
    if (isOfficial) {
        auto localPathForSharedLib = ExtensionUtils::getLocalPathForSharedLib(context);
        if (!context->getVFSUnsafe()->fileOrPathExists(localPathForSharedLib)) {
            context->getVFSUnsafe()->createDir(localPathForSharedLib);
        }
        executeExtensionLoader(context, path);
        fullPath = ExtensionUtils::getLocalPathForExtensionLib(context, path);
    }

    auto libLoader = ExtensionLibLoader(path, fullPath);
    auto init = libLoader.getInitFunc();
    (*init)(context);
    auto name = libLoader.getNameFunc();
    auto extensionName = (*name)();
    loadedExtensions.push_back(LoadedExtension(extensionName, fullPath,
        isOfficial ? ExtensionSource::OFFICIAL : ExtensionSource::USER));
}

std::string ExtensionManager::toCypher() {
    std::string cypher;
    for (auto& extension : loadedExtensions) {
        cypher += extension.toCypher();
    }
    return cypher;
}

void ExtensionManager::addExtensionOption(std::string name, common::LogicalTypeID type,
    common::Value defaultValue, bool isConfidential) {
    if (getExtensionOption(name) != nullptr) {
        // One extension option can be shared by multiple extensions.
        return;
    }
    common::StringUtils::toLower(name);
    extensionOptions.emplace(name,
        main::ExtensionOption{name, type, std::move(defaultValue), isConfidential});
}

const main::ExtensionOption* ExtensionManager::getExtensionOption(std::string name) const {
    common::StringUtils::toLower(name);
    return extensionOptions.contains(name) ? &extensionOptions.at(name) : nullptr;
}

void ExtensionManager::registerStorageExtension(std::string name,
    std::unique_ptr<storage::StorageExtension> storageExtension) {
    if (storageExtensions.contains(name)) {
        return;
    }
    storageExtensions.emplace(std::move(name), std::move(storageExtension));
}

std::vector<storage::StorageExtension*> ExtensionManager::getStorageExtensions() {
    std::vector<storage::StorageExtension*> storageExtensionsToReturn;
    for (auto& [name, storageExtension] : storageExtensions) {
        storageExtensionsToReturn.push_back(storageExtension.get());
    }
    return storageExtensionsToReturn;
}

void ExtensionManager::autoLoadLinkedExtensions(main::ClientContext* context) {
    loadLinkedExtensions(context);
}

} // namespace extension
} // namespace kuzu
