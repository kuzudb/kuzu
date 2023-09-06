#include "storage/store/table_statistics.h"

#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

TablesStatistics::TablesStatistics() {
    tablesStatisticsContentForReadOnlyTrx = std::make_unique<TablesStatisticsContent>();
}

void TablesStatistics::readFromFile(const std::string& directory) {
    readFromFile(directory, DBFileType::ORIGINAL);
}

void TablesStatistics::readFromFile(const std::string& directory, common::DBFileType dbFileType) {
    auto filePath = getTableStatisticsFilePath(directory, dbFileType);
    auto fileInfo = FileUtils::openFile(filePath, O_RDONLY);
    uint64_t offset = 0;
    uint64_t numTables;
    SerDeser::deserializeValue<uint64_t>(numTables, fileInfo.get(), offset);
    for (auto i = 0u; i < numTables; i++) {
        uint64_t numTuples;
        SerDeser::deserializeValue<uint64_t>(numTuples, fileInfo.get(), offset);
        table_id_t tableID;
        SerDeser::deserializeValue<uint64_t>(tableID, fileInfo.get(), offset);

        uint64_t numProperties;
        SerDeser::deserializeValue<uint64_t>(numProperties, fileInfo.get(), offset);
        std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>
            propertyStats;
        for (auto j = 0u; j < numProperties; j++) {
            property_id_t propertyId;
            SerDeser::deserializeValue(propertyId, fileInfo.get(), offset);
            propertyStats[propertyId] = PropertyStatistics::deserialize(fileInfo.get(), offset);
        }
        tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable[tableID] =
            deserializeTableStatistics(
                numTuples, std::move(propertyStats), offset, fileInfo.get(), tableID);
    }
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
    SerDeser::serializeValue(
        tablesStatisticsContent->tableStatisticPerTable.size(), fileInfo.get(), offset);
    for (auto& tableStatistic : tablesStatisticsContent->tableStatisticPerTable) {
        auto tableStatistics = tableStatistic.second.get();
        SerDeser::serializeValue(tableStatistics->getNumTuples(), fileInfo.get(), offset);
        SerDeser::serializeValue(tableStatistic.first, fileInfo.get(), offset);

        SerDeser::serializeValue(
            tableStatistics->getPropertyStatistics().size(), fileInfo.get(), offset);
        for (auto& propertyPair : tableStatistics->getPropertyStatistics()) {
            auto propertyId = propertyPair.first;
            auto propertyStatistics = propertyPair.second.get();
            SerDeser::serializeValue(propertyId, fileInfo.get(), offset);
            propertyStatistics->serialize(fileInfo.get(), offset);
        }

        serializeTableStatistics(tableStatistics, offset, fileInfo.get());
    }
}

void TablesStatistics::initTableStatisticsForWriteTrx() {
    std::unique_lock xLck{mtx};
    initTableStatisticsForWriteTrxNoLock();
}

void TablesStatistics::initTableStatisticsForWriteTrxNoLock() {
    if (tablesStatisticsContentForWriteTrx == nullptr) {
        tablesStatisticsContentForWriteTrx = std::make_unique<TablesStatisticsContent>();
        for (auto& tableStatistic : tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable) {
            tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableStatistic.first] =
                constructTableStatistic(tableStatistic.second.get());
        }
    }
}

} // namespace storage
} // namespace kuzu
