#include "storage/storage_manager.h"

#include <fstream>

#include "spdlog/spdlog.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/wal_replayer.h"

namespace kuzu {
namespace storage {

StorageManager::StorageManager(catalog::Catalog& catalog, BufferManager& bufferManager,
    MemoryManager& memoryManager, bool isInMemoryMode, WAL* wal)
    : logger{LoggerUtils::getOrCreateLogger("storage")}, catalog{catalog}, wal{wal} {
    logger->info("Initializing StorageManager from directory: " + wal->getDirectory());
    nodesStore = make_unique<NodesStore>(catalog, bufferManager, isInMemoryMode, wal);
    relsStore = make_unique<RelsStore>(catalog, bufferManager, memoryManager, isInMemoryMode, wal);
    nodesStore->getNodesStatisticsAndDeletedIDs().setAdjListsAndColumns(relsStore.get());
    logger->info("Done.");
}

} // namespace storage
} // namespace kuzu
