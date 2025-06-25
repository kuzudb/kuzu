#include "storage/checkpointer.h"

#include "catalog/catalog.h"
#include "common/exception/runtime.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/metadata_writer.h"
#include "main/db_config.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/shadow_utils.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"
#include "storage/storage_version_info.h"

namespace kuzu {
namespace storage {

void DatabaseHeader::updateCatalogPageRange(PageManager& pageManager, PageRange newPageRange) {
    if (catalogPageRange.startPageIdx != common::INVALID_PAGE_IDX) {
        pageManager.freePageRange(catalogPageRange);
    }
    catalogPageRange = newPageRange;
}

void DatabaseHeader::updateMetadataPageRange(PageManager& pageManager, PageRange newPageRange) {
    if (metadataPageRange.startPageIdx != common::INVALID_PAGE_IDX) {
        pageManager.freePageRange(metadataPageRange);
    }
    metadataPageRange = newPageRange;
}

static void writeMagicBytes(common::Serializer& serializer) {
    serializer.writeDebuggingInfo("magic");
    const auto numMagicBytes = strlen(StorageVersionInfo::MAGIC_BYTES);
    for (auto i = 0u; i < numMagicBytes; i++) {
        serializer.serializeValue<uint8_t>(StorageVersionInfo::MAGIC_BYTES[i]);
    }
}

void DatabaseHeader::serialize(common::Serializer& ser) const {
    writeMagicBytes(ser);
    ser.writeDebuggingInfo("storage_version");
    ser.serializeValue(StorageVersionInfo::getStorageVersion());
    ser.writeDebuggingInfo("catalog");
    ser.serializeValue(catalogPageRange.startPageIdx);
    ser.serializeValue(catalogPageRange.numPages);
    ser.writeDebuggingInfo("metadata");
    ser.serializeValue(metadataPageRange.startPageIdx);
    ser.serializeValue(metadataPageRange.numPages);
}

Checkpointer::Checkpointer(main::ClientContext& clientContext)
    : clientContext{clientContext},
      isInMemory{main::DBConfig::isDBPathInMemory(clientContext.getDatabasePath())} {}

PageRange Checkpointer::serializeCatalog(const catalog::Catalog& catalog,
    StorageManager& storageManager) const {
    auto catalogWriter = std::make_shared<common::MetaWriter>(clientContext.getMemoryManager());
    common::Serializer catalogSerializer(catalogWriter);
    catalog.serialize(catalogSerializer);
    return catalogWriter->flush(storageManager.getDataFH(), storageManager.getShadowFile());
}

PageRange Checkpointer::serializeMetadata(const catalog::Catalog& catalog,
    StorageManager& storageManager) const {
    auto metadataWriter = std::make_shared<common::MetaWriter>(clientContext.getMemoryManager());
    common::Serializer metadataSerializer(metadataWriter);
    storageManager.serialize(catalog, metadataSerializer);

    // We need to preallocate the pages for the page manager before we actually serialize it
    // This is because the page manager needs to track the pages used for itself
    // The number of pages needed for the page manager should only decrease after making an
    // additional allocation so we just calculate the number of pages needed to serialize the
    // current state of the page manager
    // Thus it is possible that we allocate an extra page that we won't end up writing to when we
    // flush the metadata writer. This may cause a discrepancy between the number of tracked pages
    // and the number of physical pages in the file but shouldn't cause any actual incorrect
    // behaviour in the database
    auto& pageManager = *storageManager.getDataFH()->getPageManager();
    const auto pagesForPageManager = pageManager.estimatePagesNeededForSerialize();
    const auto allocatedPages =
        pageManager.allocatePageRange(metadataWriter->getNumPagesToFlush() + pagesForPageManager);
    pageManager.serialize(metadataSerializer);

    metadataWriter->flush(allocatedPages, storageManager.getDataFH(),
        storageManager.getShadowFile());
    return allocatedPages;
}

void Checkpointer::writeCheckpoint() {
    if (isInMemory) {
        return;
    }
    const auto catalog = clientContext.getCatalog();
    const auto storageManager = clientContext.getStorageManager();
    auto wal = clientContext.getWAL();

    auto databaseHeader = getCurrentDatabaseHeader();

    // Checkpoint storage. Note that we first checkpoint storage before serializing the catalog, as
    // checkpointing storage may overwrite columnIDs in the catalog.
    bool hasStorageChanges = storageManager->checkpoint(&clientContext);

    auto& shadowFile = storageManager->getShadowFile();
    auto* dataFH = storageManager->getDataFH();

    // Serialize the catalog if there are changes
    if (databaseHeader.catalogPageRange.startPageIdx == common::INVALID_PAGE_IDX ||
        catalog->changedSinceLastCheckpoint()) {
        databaseHeader.updateCatalogPageRange(*dataFH->getPageManager(),
            serializeCatalog(*catalog, *storageManager));
    }
    // Serialize the storage metadata if there are changes
    if (databaseHeader.metadataPageRange.startPageIdx == common::INVALID_PAGE_IDX ||
        hasStorageChanges || catalog->changedSinceLastCheckpoint() ||
        dataFH->getPageManager()->changedSinceLastCheckpoint()) {
        databaseHeader.updateMetadataPageRange(*dataFH->getPageManager(),
            serializeMetadata(*catalog, *storageManager));
    }

    writeDatabaseHeader(databaseHeader);

    // Flush the shadow file.
    shadowFile.flushAll();

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

    // This function will evict all pages that were freed during this checkpoint
    // It must be called before we remove all evicted candidates from the BM
    // Or else the evicted pages may end up appearing multiple times in the eviction queue
    storageManager->finalizeCheckpoint();
    // When a page is freed by the FSM, it evicts it from the BM. However, if the page is freed,
    // then reused over and over, it can be appended to the eviction queue multiple times. To
    // prevent multiple entries of the same page from existing in the eviction queue, at the end of
    // each checkpoint we remove any already-evicted pages.
    auto bufferManager = clientContext.getMemoryManager()->getBufferManager();
    bufferManager->removeEvictedCandidates();

    catalog->resetVersion();
    dataFH->getPageManager()->resetVersion();
}

void Checkpointer::writeDatabaseHeader(const DatabaseHeader& header) {
    auto headerWriter = std::make_shared<common::MetaWriter>(clientContext.getMemoryManager());
    common::Serializer headerSerializer(headerWriter);
    header.serialize(headerSerializer);
    auto headerPage = headerWriter->getPage(0);

    const auto storageManager = clientContext.getStorageManager();
    auto dataFH = storageManager->getDataFH();
    auto& shadowFile = storageManager->getShadowFile();
    auto shadowHeader = ShadowUtils::createShadowVersionIfNecessaryAndPinPage(
        common::StorageConstants::DB_HEADER_PAGE_IDX, true /* skipReadingOriginalPage */, *dataFH,
        shadowFile);
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
    if (clientContext.isInMemory()) {
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

static DatabaseHeader readDatabaseHeader(common::Deserializer& deSer) {
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
    return {
        catalogPageRange,
        metaPageRange,
    };
}

DatabaseHeader Checkpointer::getCurrentDatabaseHeader() const {
    static const auto defaultHeader = DatabaseHeader{{}, {}};
    if (clientContext.getStorageManager()->getDataFH()->getFileInfo()->getFileSize() <
        common::KUZU_PAGE_SIZE) {
        // If the data file hasn't been written to there is no existing database header
        return defaultHeader;
    }
    auto vfs = clientContext.getVFSUnsafe();
    auto fileInfo = vfs->openFile(StorageUtils::getDataFName(vfs, clientContext.getDatabasePath()),
        common::FileOpenFlags{common::FileFlags::READ_ONLY}, &clientContext);
    auto reader = std::make_unique<common::BufferedFileReader>(std::move(fileInfo));
    common::Deserializer deSer(std::move(reader));
    try {
        return readDatabaseHeader(deSer);
    } catch (const common::RuntimeException&) {
        // It is possible we optimistically write to the database file before the first checkpoint
        // In this case the magic bytes check will fail and we assume there is no existing header
        return defaultHeader;
    }
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
    auto currentHeader = readDatabaseHeader(deSer);
    if (currentHeader.catalogPageRange.startPageIdx == common::INVALID_PAGE_IDX) {
        // If the catalog page range is invalid, it means there is no catalog to read, thus the
        // database is empty.
        return;
    }
    deSer.getReader()->cast<common::BufferedFileReader>()->resetReadOffset(
        currentHeader.catalogPageRange.startPageIdx * common::KUZU_PAGE_SIZE);
    catalog->deserialize(deSer);
    deSer.getReader()->cast<common::BufferedFileReader>()->resetReadOffset(
        currentHeader.metadataPageRange.startPageIdx * common::KUZU_PAGE_SIZE);
    storageManager->deserialize(context, catalog, deSer);
    storageManager->getDataFH()->getPageManager()->deserialize(deSer);
}

} // namespace storage
} // namespace kuzu
