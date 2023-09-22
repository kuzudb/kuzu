#include "storage/stats/table_statistics.h"

#include "storage/stats/nodes_statistics_and_deleted_ids.h"
#include "storage/stats/rels_statistics.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void TableStatistics::serialize(common::FileInfo* fileInfo, uint64_t& offset) {
    SerDeser::serializeValue(tableType, fileInfo, offset);
    SerDeser::serializeValue(numTuples, fileInfo, offset);
    SerDeser::serializeValue(tableID, fileInfo, offset);
    SerDeser::serializeUnorderedMap(propertyStatistics, fileInfo, offset);
    serializeInternal(fileInfo, offset);
}

std::unique_ptr<TableStatistics> TableStatistics::deserialize(
    common::FileInfo* fileInfo, uint64_t& offset) {
    TableType tableType;
    uint64_t numTuples;
    table_id_t tableID;
    std::unordered_map<property_id_t, std::unique_ptr<PropertyStatistics>> propertyStatistics;
    SerDeser::deserializeValue(tableType, fileInfo, offset);
    SerDeser::deserializeValue(numTuples, fileInfo, offset);
    SerDeser::deserializeValue(tableID, fileInfo, offset);
    SerDeser::deserializeUnorderedMap(propertyStatistics, fileInfo, offset);
    std::unique_ptr<TableStatistics> result;
    switch (tableType) {
    case TableType::NODE: {
        result = NodeTableStatsAndDeletedIDs::deserialize(tableID,
            NodeTableStatsAndDeletedIDs::getMaxNodeOffsetFromNumTuples(numTuples), fileInfo,
            offset);
    } break;
    case TableType::REL: {
        result = RelTableStats::deserialize(numTuples, tableID, fileInfo, offset);
    } break;
    // LCOV_EXCL_START
    default: {
        throw NotImplementedException("TableStatistics::deserialize");
    }
        // LCOV_EXCL_STOP
    }
    result->tableType = tableType;
    result->numTuples = numTuples;
    result->tableID = tableID;
    result->propertyStatistics = std::move(propertyStatistics);
    return result;
}

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
        for (auto& tableStatistic : tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable) {
            tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableStatistic.first] =
                constructTableStatistic(tableStatistic.second.get());
        }
    }
}

PropertyStatistics& TablesStatistics::getPropertyStatisticsForTable(
    const transaction::Transaction& transaction, common::table_id_t tableID,
    common::property_id_t propertyID) {
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

void TablesStatistics::setPropertyStatisticsForTable(common::table_id_t tableID,
    common::property_id_t propertyID, kuzu::storage::PropertyStatistics stats) {
    initTableStatisticsForWriteTrx();
    assert(tablesStatisticsContentForWriteTrx->tableStatisticPerTable.contains(tableID));
    auto tableStatistics =
        tablesStatisticsContentForWriteTrx->tableStatisticPerTable.at(tableID).get();
    tableStatistics->setPropertyStatistics(propertyID, stats);
}

} // namespace storage
} // namespace kuzu
