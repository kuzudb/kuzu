#include "storage/stats/table_statistics_collection.h"

#include <fcntl.h>

#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/store/column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

TablesStatistics::TablesStatistics(BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal)
    : metadataFH{metadataFH}, bufferManager{bufferManager}, wal{wal}, isUpdated{false} {
    readOnlyVersion = std::make_unique<TablesStatisticsContent>();
}

void TablesStatistics::readFromFile(const std::string& dbPath, FileVersionType fileVersionType,
    VirtualFileSystem* fs) {
    auto filePath = getTableStatisticsFilePath(dbPath, fileVersionType, fs);
    auto deser =
        Deserializer(std::make_unique<BufferedFileReader>(fs->openFile(filePath, O_RDONLY)));
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

PropertyStatistics& TablesStatistics::getPropertyStatisticsForTable(
    const transaction::Transaction& transaction, table_id_t tableID, property_id_t propertyID) {
    if (transaction.isReadOnly()) {
        KU_ASSERT(readOnlyVersion->tableStatisticPerTable.contains(tableID));
        auto tableStatistics = readOnlyVersion->tableStatisticPerTable.at(tableID).get();
        return tableStatistics->getPropertyStatistics(propertyID);
    } else {
        initTableStatisticsForWriteTrx();
        KU_ASSERT(readWriteVersion && readWriteVersion->tableStatisticPerTable.contains(tableID));
        auto tableStatistics = readWriteVersion->tableStatisticPerTable.at(tableID).get();
        return tableStatistics->getPropertyStatistics(propertyID);
    }
}

void TablesStatistics::setPropertyStatisticsForTable(table_id_t tableID, property_id_t propertyID,
    PropertyStatistics stats) {
    initTableStatisticsForWriteTrx();
    KU_ASSERT(readWriteVersion && readWriteVersion->tableStatisticPerTable.contains(tableID));
    setToUpdated();
    auto tableStatistics = readWriteVersion->tableStatisticPerTable.at(tableID).get();
    tableStatistics->setPropertyStatistics(propertyID, stats);
}

std::unique_ptr<MetadataDAHInfo> TablesStatistics::createMetadataDAHInfo(
    const LogicalType& dataType, BMFileHandle& metadataFH, BufferManager* bm, WAL* wal) {
    auto metadataDAHInfo = std::make_unique<MetadataDAHInfo>();
    metadataDAHInfo->dataDAHPageIdx =
        DiskArray<ColumnChunkMetadata>::addDAHPageToFile(metadataFH, bm, wal);
    metadataDAHInfo->nullDAHPageIdx =
        DiskArray<ColumnChunkMetadata>::addDAHPageToFile(metadataFH, bm, wal);
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        auto fields = StructType::getFields(dataType);
        metadataDAHInfo->childrenInfos.resize(fields.size());
        for (auto i = 0u; i < fields.size(); i++) {
            metadataDAHInfo->childrenInfos[i] =
                createMetadataDAHInfo(fields[i].getType(), metadataFH, bm, wal);
        }
    } break;
    case PhysicalTypeID::LIST: {
        metadataDAHInfo->childrenInfos.push_back(
            createMetadataDAHInfo(*LogicalType::UINT32(), metadataFH, bm, wal));
        metadataDAHInfo->childrenInfos.push_back(
            createMetadataDAHInfo(ListType::getChildType(dataType), metadataFH, bm, wal));
    } break;
    case PhysicalTypeID::ARRAY: {
        metadataDAHInfo->childrenInfos.push_back(
            createMetadataDAHInfo(*LogicalType::UINT32(), metadataFH, bm, wal));
        metadataDAHInfo->childrenInfos.push_back(
            createMetadataDAHInfo(ArrayType::getChildType(dataType), metadataFH, bm, wal));
    } break;
    case PhysicalTypeID::STRING: {
        auto dataMetadataDAHInfo = std::make_unique<MetadataDAHInfo>();
        auto offsetMetadataDAHInfo = std::make_unique<MetadataDAHInfo>();
        dataMetadataDAHInfo->dataDAHPageIdx =
            DiskArray<ColumnChunkMetadata>::addDAHPageToFile(metadataFH, bm, wal);
        offsetMetadataDAHInfo->dataDAHPageIdx =
            DiskArray<ColumnChunkMetadata>::addDAHPageToFile(metadataFH, bm, wal);
        metadataDAHInfo->childrenInfos.push_back(std::move(dataMetadataDAHInfo));
        metadataDAHInfo->childrenInfos.push_back(std::move(offsetMetadataDAHInfo));
    } break;
    default: {
        // DO NOTHING.
    }
    }
    return metadataDAHInfo;
}

} // namespace storage
} // namespace kuzu
