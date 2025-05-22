#include "storage/checkpointer.h"

#include "catalog/catalog.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/metadata_writer.h"
#include "main/db_config.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/storage_version_info.h"

namespace kuzu {
namespace storage {

Checkpointer::Checkpointer(main::ClientContext& clientContext)
    : clientContext{clientContext},
      isInMemory{main::DBConfig::isDBPathInMemory(clientContext.getDatabasePath())} {}

void Checkpointer::writeCheckpoint() {
    if (isInMemory) {
        return;
    }
    const auto catalog = clientContext.getCatalog();
    const auto storageManager = clientContext.getStorageManager();
    auto wal = clientContext.getWAL();
    // Checkpoint storage. Note that we first checkpoint storage before serializing the catalog, as
    // checkpointing storage may overwrite columnIDs in the catalog.
    storageManager->checkpoint(*catalog);

    // Serialize catalog.
    auto catalogWriter = std::make_shared<common::MetaWriter>(clientContext.getMemoryManager());
    common::Serializer catalogSerializer(catalogWriter);
    catalog->serialize(catalogSerializer);
    // Serialize storage metadata.
    auto metadataWriter = std::make_shared<common::MetaWriter>(clientContext.getMemoryManager());
    common::Serializer metadataSerializer(metadataWriter);
    storageManager->serialize(*catalog, metadataSerializer);
    // Flush the metadata to the shadow file and update the database header.
    auto dataFH = storageManager->getDataFH();
    auto& shadowFile = storageManager->getShadowFile();
    auto catalogPageRange = catalogWriter->flush(dataFH, shadowFile);
    auto metadataPageRange = metadataWriter->flush(dataFH, shadowFile);
    writeDatabaseHeader(catalogPageRange, metadataPageRange);

    // Flush the shadow file.
    shadowFile.flushAll();
    // When a page is freed by the FSM, it evicts it from the BM. However, if the page is freed,
    // then reused over and over, it can be appended to the eviction queue multiple times. To
    // prevent multiple entries of the same page from existing in the eviction queue, at the end of
    // each checkpoint we remove any already-evicted pages.
    auto bufferManager = clientContext.getMemoryManager()->getBufferManager();
    bufferManager->removeEvictedCandidates();

    // Log the checkpoint to the WAL and flush WAL. This indicates that all shadow pages and
    // files (snapshots of catalog and metadata) have been written to disk. The part that is not
    // done is to replace them with the original pages or catalog and metadata files. If the
    // system crashes before this point, the WAL can still be used to recover the system to a
    // state where the checkpoint can be redone.
    wal->logAndFlushCheckpoint();
    shadowFile.replayShadowPageRecords(clientContext);
    // Clear the wal and also shadowing files.
    wal->clearWAL();
    shadowFile.clearAll(clientContext);
    storageManager->finalizeCheckpoint();
}

static void writeMagicBytes(common::Serializer& serializer) {
    serializer.writeDebuggingInfo("magic");
    const auto numMagicBytes = strlen(StorageVersionInfo::MAGIC_BYTES);
    for (auto i = 0u; i < numMagicBytes; i++) {
        serializer.serializeValue<uint8_t>(StorageVersionInfo::MAGIC_BYTES[i]);
    }
}

void Checkpointer::writeDatabaseHeader(const PageRange& catalogPageRange,
    const PageRange& metadataPageRange) {
    auto headerWriter = std::make_shared<common::MetaWriter>(clientContext.getMemoryManager());
    common::Serializer headerSerializer(headerWriter);
    writeMagicBytes(headerSerializer);
    headerSerializer.writeDebuggingInfo("storage_version");
    headerSerializer.serializeValue(StorageVersionInfo::getStorageVersion());
    headerSerializer.writeDebuggingInfo("catalog");
    headerSerializer.serializeValue(catalogPageRange.startPageIdx);
    headerSerializer.serializeValue(catalogPageRange.numPages);
    headerSerializer.writeDebuggingInfo("metadata");
    headerSerializer.serializeValue(metadataPageRange.startPageIdx);
    headerSerializer.serializeValue(metadataPageRange.numPages);
    auto headerPage = headerWriter->getPage(0);

    const auto storageManager = clientContext.getStorageManager();
    auto dataFH = storageManager->getDataFH();
    auto& shadowFile = storageManager->getShadowFile();
    auto shadowHeader = ShadowUtils::createShadowVersionIfNecessaryAndPinPage(0,
        true /* skipReadingOriginalPage */, *dataFH, shadowFile);
    memcpy(shadowHeader.frame, headerPage.data(), common::KUZU_PAGE_SIZE);
    shadowFile.getShadowingFH().unpinPage(shadowHeader.shadowPage);
}

void Checkpointer::rollback() {
    if (isInMemory) {
        return;
    }
    const auto storageManager = clientContext.getStorageManager();
    auto catalog = clientContext.getCatalog();
    storageManager->rollbackCheckpoint(*catalog);
}

bool Checkpointer::canAutoCheckpoint(const main::ClientContext& clientContext) {
    if (main::DBConfig::isDBPathInMemory(clientContext.getDatabasePath())) {
        return false;
    }
    if (!clientContext.getDBConfig()->autoCheckpoint) {
        return false;
    }
    if (clientContext.getTransaction()->isRecovery()) {
        // Recovery transactions are not allowed to trigger auto checkpoint.
        return false;
    }
    auto wal = clientContext.getWAL();
    const auto expectedSize =
        clientContext.getTransaction()->getEstimatedMemUsage() + wal->getFileSize();
    return expectedSize > clientContext.getDBConfig()->checkpointThreshold;
}

static void validateStorageVersion(common::Deserializer& deSer) {
    std::string key;
    deSer.validateDebuggingInfo(key, "storage_version");
    storage_version_t savedStorageVersion = 0;
    deSer.deserializeValue(savedStorageVersion);
    const auto storageVersion = StorageVersionInfo::getStorageVersion();
    if (savedStorageVersion != storageVersion) {
        // TODO(Guodong): Add a test case for this.
        throw common::RuntimeException(
            common::stringFormat("Trying to read a database file with a different version. "
                                 "Database file version: {}, Current build storage version: {}",
                savedStorageVersion, storageVersion));
    }
}

static void validateMagicBytes(common::Deserializer& deSer) {
    std::string key;
    deSer.validateDebuggingInfo(key, "magic");
    const auto numMagicBytes = strlen(StorageVersionInfo::MAGIC_BYTES);
    uint8_t magicBytes[4];
    for (auto i = 0u; i < numMagicBytes; i++) {
        deSer.deserializeValue<uint8_t>(magicBytes[i]);
    }
    if (memcmp(magicBytes, StorageVersionInfo::MAGIC_BYTES, numMagicBytes) != 0) {
        throw common::RuntimeException(
            "This is not a valid Kuzu database directory for the current version of Kuzu.");
    }
}

static std::pair<PageRange, PageRange> readDatabaseHeader(common::Deserializer& deSer) {
    validateMagicBytes(deSer);
    validateStorageVersion(deSer);
    PageRange catalogPageRange{}, metaPageRange{};
    std::string key;
    deSer.validateDebuggingInfo(key, "catalog");
    deSer.deserializeValue(catalogPageRange.startPageIdx);
    deSer.deserializeValue(catalogPageRange.numPages);
    deSer.validateDebuggingInfo(key, "metadata");
    deSer.deserializeValue(metaPageRange.startPageIdx);
    deSer.deserializeValue(metaPageRange.numPages);
    return {catalogPageRange, metaPageRange};
}

void Checkpointer::readCheckpoint() {
    if (isInMemory) {
        return;
    }
    auto storageManager = clientContext.getStorageManager();
    if (storageManager->getDataFH()->getNumPages() <= 1) {
        return;
    }
    auto vfs = clientContext.getVFSUnsafe();
    readCheckpoint(clientContext.getDatabasePath(), &clientContext, vfs, clientContext.getCatalog(),
        storageManager);
}

void Checkpointer::readCheckpoint(const std::string& dbPath, main::ClientContext* context,
    common::VirtualFileSystem* vfs, catalog::Catalog* catalog, StorageManager* storageManager) {
    auto fileInfo = vfs->openFile(StorageUtils::getDataFName(vfs, dbPath),
        common::FileOpenFlags{common::FileFlags::READ_ONLY}, context);
    auto reader = std::make_unique<common::BufferedFileReader>(std::move(fileInfo));
    common::Deserializer deSer(std::move(reader));
    auto [catalogPageRange, metaPageRange] = readDatabaseHeader(deSer);
    deSer.getReader()->cast<common::BufferedFileReader>()->resetReadOffset(
        catalogPageRange.startPageIdx * common::KUZU_PAGE_SIZE);
    catalog->deserialize(deSer);
    deSer.getReader()->cast<common::BufferedFileReader>()->resetReadOffset(
        metaPageRange.startPageIdx * common::KUZU_PAGE_SIZE);
    storageManager->deserialize(*catalog, deSer);
}

} // namespace storage
} // namespace kuzu
