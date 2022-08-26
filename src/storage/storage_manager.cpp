#include "src/storage/include/storage_manager.h"

#include <fstream>

#include "spdlog/spdlog.h"

#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/include/wal_replayer.h"

namespace graphflow {
namespace storage {

StorageManager::StorageManager(
    catalog::Catalog& catalog, BufferManager& bufferManager, bool isInMemoryMode, WAL* wal)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")}, catalog{catalog}, wal{wal} {
    logger->info("Initializing StorageManager from directory: " + wal->getDirectory());
    nodesStore = make_unique<NodesStore>(catalog, bufferManager, isInMemoryMode, wal);
    relsStore =
        make_unique<RelsStore>(catalog, nodesStore->getNodesMetadata().getMaxNodeOffsetPerTable(),
            bufferManager, isInMemoryMode, wal);
    nodesStore->getNodesMetadata().setAdjListsAndColumns(relsStore.get());
    logger->info("Done.");
}

StorageManager::~StorageManager() {
    spdlog::drop("storage");
}

} // namespace storage
} // namespace graphflow
