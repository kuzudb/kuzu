#include "src/storage/include/storage_manager.h"

#include <fstream>

#include "spdlog/spdlog.h"

#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/wal_replayer.h"

namespace graphflow {
namespace storage {

StorageManager::StorageManager(const catalog::Catalog& catalog, BufferManager& bufferManager,
    string directory, bool isInMemoryMode)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")},
      bufferManager{bufferManager}, directory{move(directory)}, isInMemoryMode{isInMemoryMode} {
    logger->info("Initializing StorageManager from directory: " + this->directory);
    wal = make_unique<storage::WAL>(
        FileUtils::joinPath(this->directory, string(StorageConfig::WAL_FILE_SUFFIX)),
        bufferManager);
    nodesStore =
        make_unique<NodesStore>(catalog, bufferManager, this->directory, isInMemoryMode, wal.get());
    relsStore =
        make_unique<RelsStore>(catalog, bufferManager, this->directory, isInMemoryMode, wal.get());
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
        (isCheckpoint ? string(" checkpointing") : string(" rolling back the wal contents")) +
        " in the storage manager.");
    WALReplayer walReplayer(*this, bufferManager, isCheckpoint);
    walReplayer.replay();
    logger->info(
        "Finished " +
        (isCheckpoint ? string(" checkpointing") : string(" rolling back the wal contents")) +
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
