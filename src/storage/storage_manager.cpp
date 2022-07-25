#include "src/storage/include/storage_manager.h"

#include <fstream>

#include "spdlog/spdlog.h"

#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/include/wal_replayer.h"

namespace graphflow {
namespace storage {

StorageManager::StorageManager(
    catalog::Catalog& catalog, BufferManager& bufferManager, string directory, bool isInMemoryMode)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")},
      bufferManager{bufferManager}, directory{directory}, catalog{catalog} {
    logger->info("Initializing StorageManager from directory: " + directory);
    wal = make_unique<storage::WAL>(directory, bufferManager);
    recoverIfNecessary();
    nodesStore =
        make_unique<NodesStore>(catalog, bufferManager, directory, isInMemoryMode, wal.get());
    vector<uint64_t> maxNodeOffsetPerLabel =
        nodesStore->getNodesMetadata().getMaxNodeOffsetPerLabel();
    relsStore = make_unique<RelsStore>(
        catalog, maxNodeOffsetPerLabel, bufferManager, directory, isInMemoryMode, wal.get());
    nodesStore->getNodesMetadata().setAdjListsAndColumns(relsStore.get());
    // TODO(ZIYI): we currently don't add nodes created by DDL to the nodeStore. When restarting
    // database, we can't rebuild the nodeStore based on the replayed catalog. For now, we
    // should just rebuild catalog after building nodeStore. The next PR will implement the logic
    // to add a new node to nodeStore, then this code can be removed.
    catalog.getReadOnlyVersion()->readFromFile(directory);
    logger->info("Done.");
}

StorageManager::~StorageManager() {
    spdlog::drop("storage");
}

void StorageManager::checkpointOrRollbackAndClearWAL(bool isRecovering, bool isCheckpoint) {
    lock_t lck{checkpointMtx};
    logger->info(
        "Starting " +
        (isCheckpoint ? string("checkpointing") : string("rolling back the wal contents")) +
        " in the storage manager during " +
        (isRecovering ? "recovery." : "normal db execution (i.e., not recovering)."));
    WALReplayer walReplayer =
        isRecovering ? WALReplayer(this) : WALReplayer(this, &bufferManager, isCheckpoint);
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
            checkpointOrRollbackAndClearWAL(true /* is recovering */, true /* checkpoint */);
        } else {
            logger->info("Starting up StorageManager and found a non-empty WAL but last record is "
                         "not commit. Clearing the WAL.");
            wal->clearWAL();
        }
    }
}

} // namespace storage
} // namespace graphflow
