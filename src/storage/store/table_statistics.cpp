#include "include/table_statistics.h"

#include "src/storage/include/storage_utils.h"

namespace graphflow {
namespace storage {

TablesStatistics::TablesStatistics() {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    tableStatisticPerTableForReadOnlyTrx = make_unique<vector<unique_ptr<TableStatistics>>>();
}

void TablesStatistics::readFromFile(const string& directory) {
    auto filePath = getTableStatisticsFilePath(directory, DBFileType::ORIGINAL);
    auto fileInfo = FileUtils::openFile(filePath, O_RDONLY);
    logger->info("Reading {} from {}.", getTableTypeForPrinting(), filePath);
    uint64_t offset = 0;
    uint64_t numTables;
    offset = SerDeser::deserializeValue<uint64_t>(numTables, fileInfo.get(), offset);
    for (auto tableID = 0u; tableID < numTables; tableID++) {
        uint64_t numTuples;
        offset = SerDeser::deserializeValue<uint64_t>(numTuples, fileInfo.get(), offset);
        tableStatisticPerTableForReadOnlyTrx->push_back(
            deserializeTableStatistics(numTuples, offset, fileInfo.get(), tableID));
    }
    FileUtils::closeFile(fileInfo->fd);
}

void TablesStatistics::saveToFile(
    const string& directory, DBFileType dbFileType, TransactionType transactionType) {
    auto filePath = getTableStatisticsFilePath(directory, dbFileType);
    logger->info("Writing {} to {}.", getTableTypeForPrinting(), filePath);
    auto fileInfo = FileUtils::openFile(filePath, O_WRONLY | O_CREAT);

    uint64_t offset = 0;
    auto& tableStatisticPerTable = (transactionType == TransactionType::READ_ONLY ||
                                       tableStatisticPerTableForWriteTrx == nullptr) ?
                                       tableStatisticPerTableForReadOnlyTrx :
                                       tableStatisticPerTableForWriteTrx;
    offset = SerDeser::serializeValue(tableStatisticPerTable->size(), fileInfo.get(), offset);
    for (table_id_t tableID = 0; tableID < tableStatisticPerTable->size(); ++tableID) {
        auto tableStatistics = (*tableStatisticPerTable)[tableID].get();
        offset = SerDeser::serializeValue(tableStatistics->getNumTuples(), fileInfo.get(), offset);
        serializeTableStatistics(tableStatistics, offset, fileInfo.get());
    }
    FileUtils::closeFile(fileInfo->fd);
    logger->info("Wrote {} to {}.", getTableTypeForPrinting(), filePath);
}

void TablesStatistics::initTableStatisticPerTableForWriteTrxIfNecessary() {
    if (tableStatisticPerTableForWriteTrx == nullptr) {
        tableStatisticPerTableForWriteTrx = make_unique<vector<unique_ptr<TableStatistics>>>();
        for (auto& tableStatistic : *tableStatisticPerTableForReadOnlyTrx) {
            tableStatisticPerTableForWriteTrx->push_back(
                constructTableStatistic(tableStatistic.get()));
        }
    }
}

} // namespace storage
} // namespace graphflow
