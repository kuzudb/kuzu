#include "storage/storage_manager.h"

#include "storage/buffer_manager/buffer_manager.h"
#include "storage/wal_replayer.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

StorageManager::StorageManager(catalog::Catalog& catalog, MemoryManager& memoryManager, WAL* wal)
    : catalog{catalog}, wal{wal} {
    nodeGroupsDataFH = memoryManager.getBufferManager()->getBMFileHandle(
        StorageUtils::getNodeGroupsDataFName(wal->getDirectory()),
        FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::VERSIONED_FILE);
    nodesStore = std::make_unique<NodesStore>(nodeGroupsDataFH.get(), catalog.getNodeGroupsMetaFH(),
        catalog, *memoryManager.getBufferManager(), wal);
    relsStore = std::make_unique<RelsStore>(catalog, memoryManager, wal);
    nodesStore->getNodesStatisticsAndDeletedIDs().setAdjListsAndColumns(relsStore.get());
}

} // namespace storage
} // namespace kuzu
