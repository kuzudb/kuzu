#include "storage/stats/rels_store_statistics.h"

#include "common/assert.h"
#include "storage/stats/rel_table_statistics.h"
#include "storage/wal/wal.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelsStoreStats::RelsStoreStats(const std::string& databasePath, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, VirtualFileSystem* fs)
    : TablesStatistics{metadataFH, bufferManager, wal} {
    if (fs->fileOrPathExists(
            StorageUtils::getRelsStatisticsFilePath(fs, databasePath, FileVersionType::ORIGINAL))) {
        readFromFile(databasePath, FileVersionType::ORIGINAL, fs);
    } else {
        saveToFile(databasePath, FileVersionType::ORIGINAL, TransactionType::READ_ONLY, fs);
    }
}

void RelsStoreStats::updateNumTuplesByValue(table_id_t relTableID, int64_t value) {
    std::unique_lock lck{mtx};
    initTableStatisticsForWriteTrxNoLock();
    KU_ASSERT(readWriteVersion && readWriteVersion->tableStatisticPerTable.contains(relTableID));
    setToUpdated();
    auto relStatistics = (RelTableStats*)readWriteVersion->tableStatisticPerTable[relTableID].get();
    auto numRelsBeforeUpdate = relStatistics->getNumTuples();
    (void)numRelsBeforeUpdate; // Avoid unused variable warning.
    KU_ASSERT(!(numRelsBeforeUpdate == 0 && value < 0));
    relStatistics->setNumTuples(numRelsBeforeUpdate + value);
    // Update the nextRelID only when we are inserting rels.
    if (value > 0) {
        increaseNextRelOffset(relTableID, value);
    }
}

offset_t RelsStoreStats::getNextRelOffset(Transaction* transaction, table_id_t tableID) {
    std::unique_lock lck{mtx};
    auto& tableStatisticContent = (transaction->isReadOnly() || readWriteVersion == nullptr) ?
                                      readOnlyVersion :
                                      readWriteVersion;
    return ((RelTableStats*)tableStatisticContent->tableStatisticPerTable.at(tableID).get())
        ->getNextRelOffset();
}

void RelsStoreStats::addMetadataDAHInfo(table_id_t tableID, const LogicalType& dataType) {
    initTableStatisticsForWriteTrx();
    KU_ASSERT(readWriteVersion && readWriteVersion->tableStatisticPerTable.contains(tableID));
    setToUpdated();
    auto tableStats =
        dynamic_cast<RelTableStats*>(readWriteVersion->tableStatisticPerTable[tableID].get());
    tableStats->addMetadataDAHInfoForColumn(
        createMetadataDAHInfo(dataType, *metadataFH, bufferManager, wal), RelDataDirection::FWD);
    tableStats->addMetadataDAHInfoForColumn(
        createMetadataDAHInfo(dataType, *metadataFH, bufferManager, wal), RelDataDirection::BWD);
}

void RelsStoreStats::removeMetadataDAHInfo(table_id_t tableID, column_id_t columnID) {
    initTableStatisticsForWriteTrx();
    KU_ASSERT(readWriteVersion && readWriteVersion->tableStatisticPerTable.contains(tableID));
    setToUpdated();
    auto tableStats =
        dynamic_cast<RelTableStats*>(readWriteVersion->tableStatisticPerTable[tableID].get());
    tableStats->removeMetadataDAHInfoForColumn(columnID, RelDataDirection::FWD);
    tableStats->removeMetadataDAHInfoForColumn(columnID, RelDataDirection::BWD);
}

MetadataDAHInfo* RelsStoreStats::getCSROffsetMetadataDAHInfo(Transaction* transaction,
    table_id_t tableID, RelDataDirection direction) {
    if (transaction->isWriteTransaction()) {
        initTableStatisticsForWriteTrx();
    }
    auto tableStats = getRelStatistics(tableID, transaction);
    return tableStats->getCSROffsetMetadataDAHInfo(direction);
}

MetadataDAHInfo* RelsStoreStats::getCSRLengthMetadataDAHInfo(Transaction* transaction,
    table_id_t tableID, RelDataDirection direction) {
    if (transaction->isWriteTransaction()) {
        initTableStatisticsForWriteTrx();
    }
    auto tableStats = getRelStatistics(tableID, transaction);
    return tableStats->getCSRLengthMetadataDAHInfo(direction);
}

MetadataDAHInfo* RelsStoreStats::getColumnMetadataDAHInfo(Transaction* transaction,
    table_id_t tableID, column_id_t columnID, RelDataDirection direction) {
    if (transaction->isWriteTransaction()) {
        initTableStatisticsForWriteTrx();
    }
    auto relTableStats = getRelStatistics(tableID, transaction);
    return relTableStats->getColumnMetadataDAHInfo(columnID, direction);
}

} // namespace storage
} // namespace kuzu
