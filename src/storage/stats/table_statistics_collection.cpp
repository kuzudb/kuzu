#include "storage/stats/table_statistics_collection.h"

#include "common/ser_deser.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/store/column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

TablesStatistics::TablesStatistics(BMFileHandle* metadataFH) : metadataFH{metadataFH} {
    tablesStatisticsContentForReadOnlyTrx = std::make_unique<TablesStatisticsContent>();
}

void TablesStatistics::readFromFile(const std::string& directory) {
    readFromFile(directory, DBFileType::ORIGINAL);
}

void TablesStatistics::readFromFile(const std::string& directory, DBFileType dbFileType) {
    auto filePath = getTableStatisticsFilePath(directory, dbFileType);
    auto fileInfo = FileUtils::openFile(filePath, O_RDONLY);
    uint64_t offset = 0;
    SerDeser::deserializeUnorderedMap(
        tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable, fileInfo.get(), offset);
}

void TablesStatistics::saveToFile(const std::string& directory, DBFileType dbFileType,
    transaction::TransactionType transactionType) {
    auto filePath = getTableStatisticsFilePath(directory, dbFileType);
    auto fileInfo = FileUtils::openFile(filePath, O_WRONLY | O_CREAT);
    uint64_t offset = 0;
    auto& tablesStatisticsContent = (transactionType == transaction::TransactionType::READ_ONLY ||
                                        tablesStatisticsContentForWriteTrx == nullptr) ?
                                        tablesStatisticsContentForReadOnlyTrx :
                                        tablesStatisticsContentForWriteTrx;
    SerDeser::serializeUnorderedMap(
        tablesStatisticsContent->tableStatisticPerTable, fileInfo.get(), offset);
}

void TablesStatistics::initTableStatisticsForWriteTrx() {
    std::unique_lock xLck{mtx};
    initTableStatisticsForWriteTrxNoLock();
}

void TablesStatistics::initTableStatisticsForWriteTrxNoLock() {
    if (tablesStatisticsContentForWriteTrx == nullptr) {
        tablesStatisticsContentForWriteTrx = std::make_unique<TablesStatisticsContent>();
        for (auto& [tableID, tableStatistic] :
            tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable) {
            tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableID] =
                tableStatistic->copy();
        }
    }
}

PropertyStatistics& TablesStatistics::getPropertyStatisticsForTable(
    const transaction::Transaction& transaction, table_id_t tableID, property_id_t propertyID) {
    if (transaction.isReadOnly()) {
        assert(tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable.contains(tableID));
        auto tableStatistics =
            tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable.at(tableID).get();
        return tableStatistics->getPropertyStatistics(propertyID);
    } else {
        initTableStatisticsForWriteTrx();
        assert(tablesStatisticsContentForWriteTrx->tableStatisticPerTable.contains(tableID));
        auto tableStatistics =
            tablesStatisticsContentForWriteTrx->tableStatisticPerTable.at(tableID).get();
        return tableStatistics->getPropertyStatistics(propertyID);
    }
}

void TablesStatistics::setPropertyStatisticsForTable(
    table_id_t tableID, property_id_t propertyID, PropertyStatistics stats) {
    initTableStatisticsForWriteTrx();
    assert(tablesStatisticsContentForWriteTrx->tableStatisticPerTable.contains(tableID));
    auto tableStatistics =
        tablesStatisticsContentForWriteTrx->tableStatisticPerTable.at(tableID).get();
    tableStatistics->setPropertyStatistics(propertyID, stats);
}

std::unique_ptr<MetadataDAHInfo> TablesStatistics::createMetadataDAHInfo(
    const LogicalType& dataType, BMFileHandle& metadataFH, BufferManager* bm, WAL* wal) {
    auto metadataDAHInfo = std::make_unique<MetadataDAHInfo>();
    metadataDAHInfo->dataDAHPageIdx =
        InMemDiskArray<ColumnChunkMetadata>::addDAHPageToFile(metadataFH, bm, wal);
    metadataDAHInfo->nullDAHPageIdx =
        InMemDiskArray<ColumnChunkMetadata>::addDAHPageToFile(metadataFH, bm, wal);
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        auto fields = StructType::getFields(&dataType);
        metadataDAHInfo->childrenInfos.resize(fields.size());
        for (auto i = 0u; i < fields.size(); i++) {
            metadataDAHInfo->childrenInfos[i] =
                createMetadataDAHInfo(*fields[i]->getType(), metadataFH, bm, wal);
        }
    } break;
    case PhysicalTypeID::VAR_LIST: {
        metadataDAHInfo->childrenInfos.push_back(
            createMetadataDAHInfo(*VarListType::getChildType(&dataType), metadataFH, bm, wal));
    } break;
    case PhysicalTypeID::STRING: {
        auto childMetadataDAHInfo = std::make_unique<MetadataDAHInfo>();
        childMetadataDAHInfo->dataDAHPageIdx =
            InMemDiskArray<OverflowColumnChunkMetadata>::addDAHPageToFile(metadataFH, bm, wal);
        metadataDAHInfo->childrenInfos.push_back(std::move(childMetadataDAHInfo));
    } break;
    default: {
        // DO NOTHING.
    }
    }
    return metadataDAHInfo;
}

} // namespace storage
} // namespace kuzu
