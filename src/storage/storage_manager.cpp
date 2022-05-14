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
    logger->info("Done.");
}

StorageManager::~StorageManager() {
    spdlog::drop("storage");
}

void StorageManager::checkpointOrRollbackWAL(bool isCheckpoint) {
    lock_t lck{checkpointMtx};
    WALReplayer walReplayer(*this, bufferManager, isCheckpoint);
    walReplayer.replay();
}

void StorageManager::checkpointWAL() {
    checkpointOrRollbackWAL(true /* isCheckpoint */);
}

void StorageManager::rollbackWAL() {
    checkpointOrRollbackWAL(false /* rolling back updates */);
}

} // namespace storage
} // namespace graphflow
