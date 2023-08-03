#include "storage/storage_manager.h"

#include "storage/buffer_manager/buffer_manager.h"
#include "storage/wal_replayer.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

StorageManager::StorageManager(Catalog& catalog, MemoryManager& memoryManager, WAL* wal)
    : catalog{catalog}, wal{wal} {
    dataFH = memoryManager.getBufferManager()->getBMFileHandle(
        StorageUtils::getDataFName(wal->getDirectory()),
        FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::VERSIONED_FILE);
    metadataFH = memoryManager.getBufferManager()->getBMFileHandle(
        StorageUtils::getMetadataFName(wal->getDirectory()),
        FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::VERSIONED_FILE);
    nodesStore = std::make_unique<NodesStore>(
        dataFH.get(), metadataFH.get(), catalog, *memoryManager.getBufferManager(), wal);
    relsStore = std::make_unique<RelsStore>(catalog, memoryManager, wal);
    nodesStore->getNodesStatisticsAndDeletedIDs().setAdjListsAndColumns(relsStore.get());
}

std::unique_ptr<MetadataDAHInfo> StorageManager::initMetadataDAHInfo(const LogicalType& dataType) {
    auto metadataDAHInfo = std::make_unique<MetadataDAHInfo>();
    metadataDAHInfo->dataDAHPageIdx = metadataFH->addNewPage();
    metadataDAHInfo->nullDAHPageIdx = metadataFH->addNewPage();
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        auto fields = StructType::getFields(&dataType);
        metadataDAHInfo->childrenInfos.resize(fields.size());
        for (auto i = 0u; i < fields.size(); i++) {
            metadataDAHInfo->childrenInfos[i] = initMetadataDAHInfo(*fields[i]->getType());
        }
    } break;
    case PhysicalTypeID::VAR_LIST: {
        metadataDAHInfo->childrenInfos.push_back(
            initMetadataDAHInfo(*VarListType::getChildType(&dataType)));
    } break;
    default: {
        // DO NOTHING.
    }
    }
    return metadataDAHInfo;
}

} // namespace storage
} // namespace kuzu
