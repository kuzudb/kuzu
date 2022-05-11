#include "src/storage/include/storage_manager.h"

#include <fstream>

#include "spdlog/spdlog.h"

#include "src/storage/include/buffer_manager.h"

namespace graphflow {
namespace storage {

StorageManager::StorageManager(const catalog::Catalog& catalog, BufferManager& bufferManager,
    string directory, bool isInMemoryMode)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")}, directory{move(directory)},
      isInMemoryMode{isInMemoryMode} {
    logger->info("Initializing StorageManager from directory: " + this->directory);
    wal = make_unique<storage::WAL>(directory + StorageConfig::WAL_FILE_SUFFIX);
    nodesStore =
        make_unique<NodesStore>(catalog, bufferManager, this->directory, isInMemoryMode, wal.get());
    relsStore =
        make_unique<RelsStore>(catalog, bufferManager, this->directory, isInMemoryMode, wal.get());
    logger->info("Done.");
}

StorageManager::~StorageManager() {
    spdlog::drop("storage");
}

} // namespace storage
} // namespace graphflow
