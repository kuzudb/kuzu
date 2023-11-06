#include "storage/storage_manager.h"

#include "storage/buffer_manager/buffer_manager.h"
#include "storage/wal_replayer.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

StorageManager::StorageManager(AccessMode accessMode, Catalog& catalog,
    MemoryManager& memoryManager, WAL* wal, bool enableCompression)
    : catalog{catalog}, memoryManager{memoryManager}, wal{wal}, enableCompression{
                                                                    enableCompression} {
    dataFH = memoryManager.getBufferManager()->getBMFileHandle(
        StorageUtils::getDataFName(wal->getDirectory()),
        accessMode == AccessMode::READ_ONLY ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                                              FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::VERSIONED_FILE);
    metadataFH = memoryManager.getBufferManager()->getBMFileHandle(
        StorageUtils::getMetadataFName(wal->getDirectory()),
        accessMode == AccessMode::READ_ONLY ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                                              FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::VERSIONED_FILE);
    nodesStore = std::make_unique<NodesStore>(dataFH.get(), metadataFH.get(), accessMode, catalog,
        *memoryManager.getBufferManager(), wal, enableCompression);
    relsStore = std::make_unique<RelsStore>(dataFH.get(), metadataFH.get(), catalog,
        *memoryManager.getBufferManager(), wal, enableCompression);
}

} // namespace storage
} // namespace kuzu
