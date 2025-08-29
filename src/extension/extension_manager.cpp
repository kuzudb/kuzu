#include "extension/extension_manager.h"

#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_utils.h"
#include "extension/extension.h"
#include "generated_extension_loader.h"
#include "mbedtls/pk.h"
#include "mbedtls/sha256.h"
#include "storage/wal/local_wal.h"

namespace kuzu {
namespace extension {

static void executeExtensionLoader(main::ClientContext* context, const std::string& extensionName) {
    auto loaderPath = ExtensionUtils::getLocalPathForExtensionLoader(context, extensionName);
    if (common::VirtualFileSystem::GetUnsafe(*context)->fileOrPathExists(loaderPath)) {
        auto libLoader = ExtensionLibLoader(extensionName, loaderPath);
        auto load = libLoader.getLoadFunc();
        (*load)(context);
    }
}

static std::string computeHashForExtensionToLoad(common::offset_t signatureOffset,
    common::FileInfo* fileInfo) {
    const auto maxLenChunks = 1024ULL * 1024ULL; // 1MB
    const auto numChunks = (signatureOffset + maxLenChunks - 1) / maxLenChunks;
    std::string hashResult;
    hashResult.reserve(ExtensionManager::SHA256_LEN * numChunks);
    std::string chunkBuffer;
    chunkBuffer.resize(ExtensionManager::SHA256_LEN);
    auto chunkData = std::make_unique<uint8_t[]>(maxLenChunks);
    for (auto i = 0u; i < signatureOffset; i += maxLenChunks) {
        auto numBytesToHash = std::min<uint64_t>(signatureOffset - i, maxLenChunks);
        fileInfo->readFile(chunkData.get(), numBytesToHash);
        mbedtls_sha256(chunkData.get(), numBytesToHash,
            reinterpret_cast<unsigned char*>(chunkBuffer.data()), 0 /* SHA256 */);
        hashResult += chunkBuffer;
    }
    std::string computedExtensionHash;
    computedExtensionHash.resize(ExtensionManager::SHA256_LEN);
    mbedtls_sha256(reinterpret_cast<const unsigned char*>(hashResult.data()), hashResult.length(),
        reinterpret_cast<unsigned char*>(computedExtensionHash.data()), 0 /* SHA256 */);
    return computedExtensionHash;
}

static void verifyByPublicKey(uint8_t* signature, const std::string& computedExtensionHash) {
    mbedtls_pk_context pk_context;
    mbedtls_pk_init(&pk_context);

    mbedtls_pk_parse_public_key(&pk_context,
        reinterpret_cast<const unsigned char*>(ExtensionManager::PUBLIC_KEY),
        strlen(ExtensionManager::PUBLIC_KEY) + 1);
    auto valid = mbedtls_pk_verify(&pk_context, MBEDTLS_MD_SHA256,
                     reinterpret_cast<const unsigned char*>(computedExtensionHash.data()),
                     computedExtensionHash.size(), signature,
                     ExtensionManager::EXTENSION_SIGNATURE_LEN) == 0;
    mbedtls_pk_free(&pk_context);
    if (!valid) {
        throw common::RuntimeException{
            "Failed to verify the extension signature.\nIf you want to load unsigned extensions, "
            "please set allow_unsigned_extension=false."};
    }
}

static std::unique_ptr<uint8_t[]> getSignature(common::FileInfo* fileInfo,
    common::offset_t signatureOffset) {
    auto signatureBuffer = std::make_unique<uint8_t[]>(ExtensionManager::EXTENSION_SIGNATURE_LEN);
    fileInfo->readFromFile(signatureBuffer.get(), ExtensionManager::EXTENSION_SIGNATURE_LEN,
        signatureOffset);
    return signatureBuffer;
}

static void validateSignature(main::ClientContext* context, const std::string& fullPath) {
    auto fileInfo = common::VirtualFileSystem::GetUnsafe(*context)->openFile(fullPath,
        common::FileOpenFlags(common::FileFlags::READ_ONLY), context);
    auto fileSize = fileInfo->getFileSize();
    if (ExtensionManager::EXTENSION_SIGNATURE_LEN >= fileSize) {
        throw common::RuntimeException{"The file is too small to be a kuzu extension."};
    }
    auto signatureOffset = fileSize - ExtensionManager::EXTENSION_SIGNATURE_LEN;
    auto signature = getSignature(fileInfo.get(), signatureOffset);
    auto computedExtensionHash = computeHashForExtensionToLoad(signatureOffset, fileInfo.get());
    verifyByPublicKey(signature.get(), computedExtensionHash);
}

void ExtensionManager::loadExtension(const std::string& path, main::ClientContext* context) {
    auto fullPath = path;
    bool isOfficial = ExtensionUtils::isOfficialExtension(path);
    if (isOfficial) {
        auto localPathForSharedLib = ExtensionUtils::getLocalPathForSharedLib(context);
        if (!common::VirtualFileSystem::GetUnsafe(*context)->fileOrPathExists(
                localPathForSharedLib)) {
            common::VirtualFileSystem::GetUnsafe(*context)->createDir(localPathForSharedLib);
        }
        executeExtensionLoader(context, path);
        fullPath = ExtensionUtils::getLocalPathForExtensionLib(context, path);
    }
    if (!context->getClientConfig()->allowUnsignedExtension) {
        validateSignature(context, path);
    }
    auto libLoader = ExtensionLibLoader(path, fullPath);
    auto name = libLoader.getNameFunc();
    std::string extensionName = (*name)();
    if (std::any_of(loadedExtensions.begin(), loadedExtensions.end(),
            [&](const LoadedExtension& ext) { return ext.getExtensionName() == extensionName; })) {
        libLoader.unload();
        throw common::BinderException{
            common::stringFormat("Extension: {} is already loaded. You can check loaded extensions "
                                 "by `CALL SHOW_LOADED_EXTENSIONS() RETURN *`.",
                extensionName)};
    }
    auto init = libLoader.getInitFunc();
    (*init)(context);
    loadedExtensions.push_back(LoadedExtension(extensionName, fullPath,
        isOfficial ? ExtensionSource::OFFICIAL : ExtensionSource::USER));
    auto transaction = context->getTransaction();
    if (transaction->shouldLogToWAL()) {
        transaction->getLocalWAL().logLoadExtension(path);
    }
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
    auto trxContext = context->getTransactionContext();
    trxContext->beginRecoveryTransaction();
    loadLinkedExtensions(context, loadedExtensions);
    trxContext->commit();
}

ExtensionManager* ExtensionManager::Get(const main::ClientContext& context) {
    return context.getDatabase()->getExtensionManager();
}

} // namespace extension
} // namespace kuzu
