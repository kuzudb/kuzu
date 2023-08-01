#include "storage/store/table_statistics.h"

#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

TablesStatistics::TablesStatistics() {
    logger = LoggerUtils::getLogger(LoggerConstants::LoggerEnum::STORAGE);
    tablesStatisticsContentForReadOnlyTrx = std::make_unique<TablesStatisticsContent>();
}

void TablesStatistics::readFromFile(const std::string& directory) {
    auto filePath = getTableStatisticsFilePath(directory, DBFileType::ORIGINAL);
    auto fileInfo = FileUtils::openFile(filePath, O_RDONLY);
    logger->info("Reading {} from {}.", getTableTypeForPrinting(), filePath);
    uint64_t offset = 0;
    uint64_t numTables;
    SerDeser::deserializeValue<uint64_t>(numTables, fileInfo.get(), offset);
    for (auto i = 0u; i < numTables; i++) {
        uint64_t numTuples;
        SerDeser::deserializeValue<uint64_t>(numTuples, fileInfo.get(), offset);
        table_id_t tableID;
        SerDeser::deserializeValue<uint64_t>(tableID, fileInfo.get(), offset);
        tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable[tableID] =
            deserializeTableStatistics(numTuples, offset, fileInfo.get(), tableID);
    }
}

void TablesStatistics::saveToFile(const std::string& directory, DBFileType dbFileType,
    transaction::TransactionType transactionType) {
    auto filePath = getTableStatisticsFilePath(directory, dbFileType);
    logger->info("Writing {} to {}.", getTableTypeForPrinting(), filePath);
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
        serializeTableStatistics(tableStatistics, offset, fileInfo.get());
    }
    logger->info("Wrote {} to {}.", getTableTypeForPrinting(), filePath);
}

void TablesStatistics::initTableStatisticPerTableForWriteTrxIfNecessary() {
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
