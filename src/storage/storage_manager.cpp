#include "storage/storage_manager.h"

#include "storage/buffer_manager/buffer_manager.h"
#include "storage/wal_replayer.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

StorageManager::StorageManager(Catalog& catalog, MemoryManager& memoryManager, WAL* wal)
    : catalog{catalog}, memoryManager{memoryManager}, wal{wal} {
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

std::unique_ptr<MetadataDAHInfo> StorageManager::createMetadataDAHInfo(
    const common::LogicalType& dataType) {
    auto metadataDAHInfo = std::make_unique<MetadataDAHInfo>();
    metadataDAHInfo->dataDAHPageIdx = InMemDiskArray<ColumnChunkMetadata>::addDAHPageToFile(
        *metadataFH, StorageStructureID{StorageStructureType::METADATA},
        memoryManager.getBufferManager(), wal);
    metadataDAHInfo->nullDAHPageIdx = InMemDiskArray<ColumnChunkMetadata>::addDAHPageToFile(
        *metadataFH, StorageStructureID{StorageStructureType::METADATA},
        memoryManager.getBufferManager(), wal);
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        auto fields = StructType::getFields(&dataType);
        metadataDAHInfo->childrenInfos.resize(fields.size());
        for (auto i = 0u; i < fields.size(); i++) {
            metadataDAHInfo->childrenInfos[i] = createMetadataDAHInfo(*fields[i]->getType());
        }
    } break;
    case PhysicalTypeID::VAR_LIST: {
        metadataDAHInfo->childrenInfos.push_back(
            createMetadataDAHInfo(*VarListType::getChildType(&dataType)));
    } break;
    case PhysicalTypeID::STRING: {
        auto dummyChildType = LogicalType{LogicalTypeID::ANY};
        metadataDAHInfo->childrenInfos.push_back(createMetadataDAHInfo(dummyChildType));
    } break;
    default: {
        // DO NOTHING.
    }
    }
    return metadataDAHInfo;
}

} // namespace storage
} // namespace kuzu
