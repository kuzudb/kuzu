#include "storage/stats/table_statistics_collection.h"

#include <fcntl.h>

#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "storage/storage_structure//disk_array_collection.h"
#include "storage/store/string_column.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

TablesStatistics::TablesStatistics(DiskArrayCollection& metadataDAC, BufferManager* bufferManager,
    WAL* wal)
    : metadataDAC{metadataDAC}, bufferManager{bufferManager}, wal{wal}, isUpdated{false} {
    readOnlyVersion = std::make_unique<TablesStatisticsContent>();
}

void TablesStatistics::readFromFile(const std::string& dbPath, FileVersionType fileVersionType,
    VirtualFileSystem* fs, main::ClientContext* context) {
    auto filePath = getTableStatisticsFilePath(dbPath, fileVersionType, fs);
    auto deser = Deserializer(
        std::make_unique<BufferedFileReader>(fs->openFile(filePath, O_RDONLY, context)));
    deser.deserializeUnorderedMap(readOnlyVersion->tableStatisticPerTable);
}

void TablesStatistics::saveToFile(const std::string& directory, FileVersionType fileVersionType,
    transaction::TransactionType transactionType, VirtualFileSystem* fs) {
    auto filePath = getTableStatisticsFilePath(directory, fileVersionType, fs);
    auto ser = Serializer(
        std::make_unique<BufferedFileWriter>(fs->openFile(filePath, O_WRONLY | O_CREAT)));
    auto& tablesStatisticsContent = (transactionType == transaction::TransactionType::READ_ONLY ||
                                        readWriteVersion == nullptr) ?
                                        readOnlyVersion :
                                        readWriteVersion;
    ser.serializeUnorderedMap(tablesStatisticsContent->tableStatisticPerTable);
}

void TablesStatistics::initTableStatisticsForWriteTrx() {
    std::unique_lock xLck{mtx};
    initTableStatisticsForWriteTrxNoLock();
}

void TablesStatistics::initTableStatisticsForWriteTrxNoLock() {
    if (readWriteVersion == nullptr) {
        readWriteVersion = std::make_unique<TablesStatisticsContent>();
        KU_ASSERT(readOnlyVersion);
        for (auto& [tableID, tableStatistic] : readOnlyVersion->tableStatisticPerTable) {
            readWriteVersion->tableStatisticPerTable[tableID] = tableStatistic->copy();
        }
    }
}

std::unique_ptr<MetadataDAHInfo> TablesStatistics::createMetadataDAHInfo(
    const LogicalType& dataType, DiskArrayCollection& metadataDAC) {
    auto metadataDAHInfo = std::make_unique<MetadataDAHInfo>();
    metadataDAHInfo->dataDAHIdx = metadataDAC.addDiskArray();
    metadataDAHInfo->nullDAHIdx = metadataDAC.addDiskArray();
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        const auto& fields = StructType::getFields(dataType);
        metadataDAHInfo->childrenInfos.resize(fields.size());
        for (auto i = 0u; i < fields.size(); i++) {
            metadataDAHInfo->childrenInfos[i] =
                createMetadataDAHInfo(fields[i].getType(), metadataDAC);
        }
    } break;
    case PhysicalTypeID::LIST: {
        metadataDAHInfo->childrenInfos.push_back(
            createMetadataDAHInfo(LogicalType::UINT32(), metadataDAC));
        metadataDAHInfo->childrenInfos.push_back(
            createMetadataDAHInfo(ListType::getChildType(dataType), metadataDAC));
        metadataDAHInfo->childrenInfos.push_back(
            createMetadataDAHInfo(LogicalType::UINT64(), metadataDAC));
    } break;
    case PhysicalTypeID::ARRAY: {
        metadataDAHInfo->childrenInfos.push_back(
            createMetadataDAHInfo(LogicalType::UINT32(), metadataDAC));
        metadataDAHInfo->childrenInfos.push_back(
            createMetadataDAHInfo(ArrayType::getChildType(dataType), metadataDAC));
        metadataDAHInfo->childrenInfos.push_back(
            createMetadataDAHInfo(LogicalType::UINT64(), metadataDAC));
    } break;
    case PhysicalTypeID::STRING: {
        metadataDAHInfo->childrenInfos.resize(StringColumn::CHILD_STATE_COUNT);

        auto dataMetadataDAHInfo = std::make_unique<MetadataDAHInfo>();
        auto offsetMetadataDAHInfo = std::make_unique<MetadataDAHInfo>();
        dataMetadataDAHInfo->dataDAHIdx = metadataDAC.addDiskArray();
        offsetMetadataDAHInfo->dataDAHIdx = metadataDAC.addDiskArray();
        metadataDAHInfo
            ->childrenInfos[static_cast<common::idx_t>(StringColumn::ChildStateIndex::DATA)] =
            std::move(dataMetadataDAHInfo);
        metadataDAHInfo
            ->childrenInfos[static_cast<common::idx_t>(StringColumn::ChildStateIndex::OFFSET)] =
            std::move(offsetMetadataDAHInfo);

        auto indexMetadataDAHInfo = std::make_unique<MetadataDAHInfo>();
        indexMetadataDAHInfo->dataDAHIdx = metadataDAC.addDiskArray();
        metadataDAHInfo
            ->childrenInfos[static_cast<common::idx_t>(StringColumn::ChildStateIndex::INDEX)] =
            std::move(indexMetadataDAHInfo);
    } break;
    default: {
        // DO NOTHING.
    }
    }
    return metadataDAHInfo;
}

} // namespace storage
} // namespace kuzu
