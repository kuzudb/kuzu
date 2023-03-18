#include "storage/storage_manager.h"

#include <fstream>

#include "storage/buffer_manager/buffer_manager.h"
#include "storage/wal_replayer.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

StorageManager::StorageManager(catalog::Catalog& catalog, MemoryManager& memoryManager, WAL* wal)
    : logger{LoggerUtils::getLogger(LoggerConstants::LoggerEnum::STORAGE)}, catalog{catalog},
      wal{wal} {
    logger->info("Initializing StorageManager from directory: " + wal->getDirectory());
    nodesStore = std::make_unique<NodesStore>(catalog, *memoryManager.getBufferManager(), wal);
    relsStore = std::make_unique<RelsStore>(catalog, memoryManager, wal);
    nodesStore->getNodesStatisticsAndDeletedIDs().setAdjListsAndColumns(relsStore.get());
    logger->info("Done.");
}

} // namespace storage
} // namespace kuzu
