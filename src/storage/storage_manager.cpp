#include "src/storage/include/storage_manager.h"

#include <fstream>

#include "spdlog/spdlog.h"

#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/include/wal_replayer.h"

namespace graphflow {
namespace storage {

StorageManager::StorageManager(const catalog::Catalog& catalog, BufferManager& bufferManager,
    string directory, bool isInMemoryMode)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")},
      bufferManager{bufferManager}, directory{move(directory)} {
    logger->info("Initializing StorageManager from directory: " + this->directory);
    wal = make_unique<storage::WAL>(this->directory, bufferManager);
    // We do part of the recovery first to make sure that the NodesMetadata file's disk contents
    // are accurate. We achieve atomicity for NodesMetadata file by writing it to a backup file,
    // and don't have a mechanism to refresh it (we could construct and reconstruct it but
    // we do not currently do that).
    if (wal->isLastLoggedRecordCommit() && wal->containsNodesMetadataRecord()) {
        StorageUtils::overwriteNodesMetadataFileWithVersionFromWAL(directory);
    }
    nodesStore =
        make_unique<NodesStore>(catalog, bufferManager, this->directory, isInMemoryMode, wal.get());
    // We keep a reference here and pass to relsStore.
    vector<uint64_t> maxNodeOffsetPerLabel =
        nodesStore->getNodesMetadata().getMaxNodeOffsetPerLabel();
    relsStore = make_unique<RelsStore>(
        catalog, maxNodeOffsetPerLabel, bufferManager, this->directory, isInMemoryMode, wal.get());
    nodesStore->getNodesMetadata().setAdjListsAndColumns(relsStore.get());
    recoverIfNecessary();
    logger->info("Done.");
}

StorageManager::~StorageManager() {
    spdlog::drop("storage");
}

void StorageManager::checkpointOrRollbackAndClearWAL(bool isCheckpoint) {
    lock_t lck{checkpointMtx};
    logger->info(
        "Starting " +
        (isCheckpoint ? string("checkpointing") : string("rolling back the wal contents")) +
        " in the storage manager.");
    WALReplayer walReplayer(*this, bufferManager, isCheckpoint);
    walReplayer.replay();
    logger->info(
        "Finished " +
        (isCheckpoint ? string("checkpointing") : string("rolling back the wal contents")) +
        " in the storage manager.");
    wal->clearWAL();
}

void StorageManager::recoverIfNecessary() {
    if (!wal->isEmptyWAL()) {
        if (wal->isLastLoggedRecordCommit()) {
            logger->info("Starting up StorageManager and found a non-empty WAL with a committed "
                         "transaction. Replaying to checkpoint.");
            checkpointOrRollbackAndClearWAL(true /* checkpoint */);
        } else {
            logger->info("Starting up StorageManager and found a non-empty WAL but last record is "
                         "not commit. Clearing the WAL.");
            wal->clearWAL();
        }
    }
}

} // namespace storage
} // namespace graphflow
