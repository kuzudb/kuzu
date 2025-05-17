#include "storage/checkpointer.h"

#include "catalog/catalog.h"
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
    auto metadataWriter =
        std::make_shared<common::MetadataWriter>(clientContext.getMemoryManager());
    common::Serializer metadataSerializer(metadataWriter);
    catalog->serialize(metadataSerializer);
    // Checkpoint tables, which writes the updated/newly inserted pages and metadata to disk.
    storageManager->checkpoint(*catalog, metadataSerializer);
    // Flush the metadata to the shadow file.
    auto dataFH = storageManager->getDataFH();
    auto& shadowFile = storageManager->getShadowFile();
    auto metaPageRange = metadataWriter->flush(dataFH, shadowFile);
    writeDatabaseHeader(metaPageRange);

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
    const auto numMagicBytes = strlen(StorageVersionInfo::MAGIC_BYTES);
    for (auto i = 0u; i < numMagicBytes; i++) {
        serializer.serializeValue<uint8_t>(StorageVersionInfo::MAGIC_BYTES[i]);
    }
}

void Checkpointer::writeDatabaseHeader(const PageRange& metaPageRange) {
    auto headerWriter = std::make_shared<common::MetadataWriter>(clientContext.getMemoryManager());
    common::Serializer headerSerializer(headerWriter);
    writeMagicBytes(headerSerializer);
    headerSerializer.serializeValue(StorageVersionInfo::getStorageVersion());
    headerSerializer.serializeValue(metaPageRange.startPageIdx);
    headerSerializer.serializeValue(metaPageRange.numPages);
    auto headerPage = headerWriter->getPage(0);

    const auto storageManager = clientContext.getStorageManager();
    auto dataFH = storageManager->getDataFH();
    auto& shadowFile = storageManager->getShadowFile();
    auto isHeaderNewlyAppendedPage = dataFH->getNumPages() == 0;
    auto shadowHeader = ShadowUtils::createShadowVersionIfNecessaryAndPinPage(0,
        isHeaderNewlyAppendedPage, *dataFH, DBFileID::newDataFileID(), shadowFile);
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

static PageRange readDatabaseHeader(common::Deserializer& deSer) {}

void Checkpointer::readCheckpoint() {}

} // namespace storage
} // namespace kuzu
