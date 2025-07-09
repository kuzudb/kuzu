#include "storage/checkpointer.h"

#include "catalog/catalog.h"
#include "common/exception/runtime.h"
#include "common/file_system/file_system.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/in_mem_file_writer.h"
#include "extension/extension_manager.h"
#include "main/db_config.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/shadow_utils.h"
#include "storage/storage_manager.h"
#include "storage/storage_version_info.h"
#include "storage/wal/local_wal.h"

namespace kuzu {
namespace storage {

void DatabaseHeader::updateCatalogPageRange(PageManager& pageManager, PageRange newPageRange) {
    if (catalogPageRange.startPageIdx != common::INVALID_PAGE_IDX) {
        pageManager.freePageRange(catalogPageRange);
    }
    catalogPageRange = newPageRange;
}

void DatabaseHeader::freeMetadataPageRange(PageManager& pageManager) const {
    if (metadataPageRange.startPageIdx != common::INVALID_PAGE_IDX) {
        pageManager.freePageRange(metadataPageRange);
    }
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
      isInMemory{main::DBConfig::isDBPathInMemory(clientContext.getDatabasePath())},
      pageAllocator(*clientContext.getStorageManager()->getDataFH()->getPageManager()) {}

Checkpointer::~Checkpointer() = default;

PageRange Checkpointer::serializeCatalog(const catalog::Catalog& catalog,
    StorageManager& storageManager) {
    auto catalogWriter =
        std::make_shared<common::InMemFileWriter>(*clientContext.getMemoryManager());
    common::Serializer catalogSerializer(catalogWriter);
    catalog.serialize(catalogSerializer);
    return catalogWriter->flush(pageAllocator, storageManager.getShadowFile());
}

PageRange Checkpointer::serializeMetadata(const catalog::Catalog& catalog,
    StorageManager& storageManager) {
    auto metadataWriter =
        std::make_shared<common::InMemFileWriter>(*clientContext.getMemoryManager());
    common::Serializer metadataSerializer(metadataWriter);
    storageManager.serialize(catalog, metadataSerializer);

    // We need to preallocate the pages for the page manager before we actually serialize it,
    // this is because the page manager needs to track the pages used for itself.
    // The number of pages needed for the page manager should only decrease after making an
    // additional allocation, so we just calculate the number of pages needed to serialize the
    // current state of the page manager.
    // Thus, it is possible that we allocate an extra page that we won't end up writing to when we
    // flush the metadata writer. This may cause a discrepancy between the number of tracked pages
    // and the number of physical pages in the file but shouldn't cause any actual incorrect
    // behavior in the database.
    auto& pageManager = *storageManager.getDataFH()->getPageManager();
    const auto pagesForPageManager = pageManager.estimatePagesNeededForSerialize();
    const auto allocatedPages =
        pageAllocator.allocatePageRange(metadataWriter->getNumPagesToFlush() + pagesForPageManager);
    pageManager.serialize(metadataSerializer);

    metadataWriter->flush(allocatedPages, pageAllocator.getDataFH(),
        storageManager.getShadowFile());
    return allocatedPages;
}

void Checkpointer::writeCheckpoint() {
    if (isInMemory) {
        return;
    }

    auto databaseHeader = getCurrentDatabaseHeader();
    // Checkpoint storage. Note that we first checkpoint storage before serializing the catalog, as
    // checkpointing storage may overwrite columnIDs in the catalog.
    bool hasStorageChanges = checkpointStorage();
    serializeCatalogAndMetadata(databaseHeader, hasStorageChanges);
    writeDatabaseHeader(databaseHeader);
    logCheckpointAndApplyShadowPages();

    // This function will evict all pages that were freed during this checkpoint
    // It must be called before we remove all evicted candidates from the BM
    // Or else the evicted pages may end up appearing multiple times in the eviction queue
    auto storageManager = clientContext.getStorageManager();
    storageManager->finalizeCheckpoint();
    // When a page is freed by the FSM, it evicts it from the BM. However, if the page is freed,
    // then reused over and over, it can be appended to the eviction queue multiple times. To
    // prevent multiple entries of the same page from existing in the eviction queue, at the end of
    // each checkpoint we remove any already-evicted pages.
    auto bufferManager = clientContext.getMemoryManager()->getBufferManager();
    bufferManager->removeEvictedCandidates();

    clientContext.getCatalog()->resetVersion();
    auto* dataFH = storageManager->getDataFH();
    dataFH->getPageManager()->resetVersion();
    storageManager->getWAL().reset();
    storageManager->getShadowFile().reset();
}

bool Checkpointer::checkpointStorage() {
    const auto storageManager = clientContext.getStorageManager();
    return storageManager->checkpoint(&clientContext, pageAllocator);
}

void Checkpointer::serializeCatalogAndMetadata(DatabaseHeader& databaseHeader,
    bool hasStorageChanges) {
    const auto storageManager = clientContext.getStorageManager();
    const auto catalog = clientContext.getCatalog();
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
        // We must free the existing metadata page range before serializing
        // So that the freed pages are serialized by the FSM
        databaseHeader.freeMetadataPageRange(*dataFH->getPageManager());
        databaseHeader.metadataPageRange = serializeMetadata(*catalog, *storageManager);
    }
}

void Checkpointer::writeDatabaseHeader(const DatabaseHeader& header) {
    auto headerWriter =
        std::make_shared<common::InMemFileWriter>(*clientContext.getMemoryManager());
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

void Checkpointer::logCheckpointAndApplyShadowPages() {
    const auto storageManager = clientContext.getStorageManager();
    auto& shadowFile = storageManager->getShadowFile();
    // Flush the shadow file.
    shadowFile.flushAll();
    auto wal = clientContext.getWAL();
    // Log the checkpoint to the WAL and flush WAL. This indicates that all shadow pages and
    // files (snapshots of catalog and metadata) have been written to disk. The part that is not
    // done is to replace them with the original pages or catalog and metadata files. If the
    // system crashes before this point, the WAL can still be used to recover the system to a
    // state where the checkpoint can be redone.
    wal->logAndFlushCheckpoint(&clientContext);
    shadowFile.applyShadowPages(clientContext);
    // Clear the wal and also shadowing files.
    auto bufferManager = clientContext.getMemoryManager()->getBufferManager();
    wal->clear();
    shadowFile.clear(*bufferManager);
}

void Checkpointer::rollback() {
    if (isInMemory) {
        return;
    }
    const auto storageManager = clientContext.getStorageManager();
    auto catalog = clientContext.getCatalog();
    // Any pages freed during the checkpoint are no longer freed
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
        clientContext.getTransaction()->getLocalWAL().getSize() + wal->getFileSize();
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
            "Unable to open database. The file is not a valid Kuzu database file!");
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
    auto fileInfo = vfs->openFile(clientContext.getDatabasePath(),
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
    auto storageManager = clientContext.getStorageManager();
    if (!isInMemory && storageManager->getDataFH()->getNumPages() > 0) {
        auto vfs = clientContext.getVFSUnsafe();
        readCheckpoint(clientContext.getDatabasePath(), &clientContext, vfs,
            clientContext.getCatalog(), clientContext.getStorageManager());
    }
    clientContext.getExtensionManager()->autoLoadLinkedExtensions(&clientContext);
}

void Checkpointer::readCheckpoint(const std::string& dbPath, main::ClientContext* context,
    common::VirtualFileSystem* vfs, catalog::Catalog* catalog, StorageManager* storageManager) {
    auto fileInfo =
        vfs->openFile(dbPath, common::FileOpenFlags{common::FileFlags::READ_ONLY}, context);
    auto reader = std::make_unique<common::BufferedFileReader>(std::move(fileInfo));
    common::Deserializer deSer(std::move(reader));
    auto currentHeader = readDatabaseHeader(deSer);
    if (currentHeader.catalogPageRange.startPageIdx == common::INVALID_PAGE_IDX) {
        // If the catalog page range is invalid, it means there is no catalog to read; thus, the
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
