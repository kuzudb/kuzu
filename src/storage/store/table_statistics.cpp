#include "storage/store/table_statistics.h"

#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

TablesStatistics::TablesStatistics() {
    logger = LoggerUtils::getOrCreateLogger("storage");
    tablesStatisticsContentForReadOnlyTrx = make_unique<TablesStatisticsContent>();
}

void TablesStatistics::readFromFile(const string& directory) {
    auto filePath = getTableStatisticsFilePath(directory, DBFileType::ORIGINAL);
    auto fileInfo = FileUtils::openFile(filePath, O_RDONLY);
    logger->info("Reading {} from {}.", getTableTypeForPrinting(), filePath);
    uint64_t offset = 0;
    uint64_t numTables;
    offset = SerDeser::deserializeValue(
        tablesStatisticsContentForReadOnlyTrx->nextRelID, fileInfo.get(), offset);
    offset = SerDeser::deserializeValue<uint64_t>(numTables, fileInfo.get(), offset);
    for (auto i = 0u; i < numTables; i++) {
        uint64_t numTuples;
        offset = SerDeser::deserializeValue<uint64_t>(numTuples, fileInfo.get(), offset);
        table_id_t tableID;
        offset = SerDeser::deserializeValue<uint64_t>(tableID, fileInfo.get(), offset);
        tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable[tableID] =
            deserializeTableStatistics(numTuples, offset, fileInfo.get(), tableID);
    }
}

void TablesStatistics::saveToFile(
    const string& directory, DBFileType dbFileType, TransactionType transactionType) {
    auto filePath = getTableStatisticsFilePath(directory, dbFileType);
    logger->info("Writing {} to {}.", getTableTypeForPrinting(), filePath);
    auto fileInfo = FileUtils::openFile(filePath, O_WRONLY | O_CREAT);
    uint64_t offset = 0;
    auto& tablesStatisticsContent = (transactionType == TransactionType::READ_ONLY ||
                                        tablesStatisticsContentForWriteTrx == nullptr) ?
                                        tablesStatisticsContentForReadOnlyTrx :
                                        tablesStatisticsContentForWriteTrx;
    offset = SerDeser::serializeValue(tablesStatisticsContent->nextRelID, fileInfo.get(), offset);
    offset = SerDeser::serializeValue(
        tablesStatisticsContent->tableStatisticPerTable.size(), fileInfo.get(), offset);
    for (auto& tableStatistic : tablesStatisticsContent->tableStatisticPerTable) {
        auto tableStatistics = tableStatistic.second.get();
        offset = SerDeser::serializeValue(tableStatistics->getNumTuples(), fileInfo.get(), offset);
        offset = SerDeser::serializeValue(tableStatistic.first, fileInfo.get(), offset);
        serializeTableStatistics(tableStatistics, offset, fileInfo.get());
    }
    logger->info("Wrote {} to {}.", getTableTypeForPrinting(), filePath);
}

void TablesStatistics::initTableStatisticPerTableForWriteTrxIfNecessary() {
    if (tablesStatisticsContentForWriteTrx == nullptr) {
        tablesStatisticsContentForWriteTrx = make_unique<TablesStatisticsContent>();
        tablesStatisticsContentForWriteTrx->nextRelID =
            tablesStatisticsContentForReadOnlyTrx->nextRelID;
        for (auto& tableStatistic : tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable) {
            tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableStatistic.first] =
                constructTableStatistic(tableStatistic.second.get());
        }
    }
}

} // namespace storage
} // namespace kuzu
