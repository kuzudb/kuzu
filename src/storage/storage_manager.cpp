#include "storage/storage_manager.h"

#include "storage/stats/nodes_store_statistics.h"
#include "storage/wal_replayer.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

StorageManager::StorageManager(bool readOnly, const Catalog& catalog, MemoryManager& memoryManager,
    WAL* wal, bool enableCompression)
    : memoryManager{memoryManager}, wal{wal}, enableCompression{enableCompression} {
    dataFH = memoryManager.getBufferManager()->getBMFileHandle(
        StorageUtils::getDataFName(wal->getDirectory()),
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::VERSIONED_FILE);
    metadataFH = memoryManager.getBufferManager()->getBMFileHandle(
        StorageUtils::getMetadataFName(wal->getDirectory()),
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::VERSIONED_FILE);
    nodesStatisticsAndDeletedIDs = std::make_unique<NodesStoreStatsAndDeletedIDs>(
        metadataFH.get(), memoryManager.getBufferManager(), wal);
    relsStatistics =
        std::make_unique<RelsStoreStats>(metadataFH.get(), memoryManager.getBufferManager(), wal);
    loadTables(readOnly, catalog);
}

void StorageManager::loadTables(bool readOnly, const catalog::Catalog& catalog) {
    for (auto& schema : catalog.getReadOnlyVersion()->getNodeTableSchemas()) {
        KU_ASSERT(!tables.contains(schema->tableID));
        auto nodeTableSchema = reinterpret_cast<NodeTableSchema*>(schema);
        tables[schema->tableID] = std::make_unique<NodeTable>(dataFH.get(), metadataFH.get(),
            nodeTableSchema, nodesStatisticsAndDeletedIDs.get(), &memoryManager, wal, readOnly,
            enableCompression);
    }
    for (auto schema : catalog.getReadOnlyVersion()->getRelTableSchemas()) {
        KU_ASSERT(!tables.contains(schema->tableID));
        auto relTableSchema = dynamic_cast<RelTableSchema*>(schema);
        tables[schema->tableID] = std::make_unique<RelTable>(dataFH.get(), metadataFH.get(),
            relsStatistics.get(), &memoryManager, relTableSchema, wal, enableCompression);
    }
}

void StorageManager::createTable(common::table_id_t tableID, catalog::Catalog* catalog) {
    auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(tableID);
    switch (tableSchema->tableType) {
    case TableType::NODE: {
        auto nodeTableSchema = reinterpret_cast<NodeTableSchema*>(tableSchema);
        tables[tableID] = std::make_unique<NodeTable>(dataFH.get(), metadataFH.get(),
            nodeTableSchema, nodesStatisticsAndDeletedIDs.get(), &memoryManager, wal,
            false /* readOnly */, enableCompression);
    } break;
    case TableType::REL: {
        auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
        tables[tableID] = std::make_unique<RelTable>(dataFH.get(), metadataFH.get(),
            relsStatistics.get(), &memoryManager, relTableSchema, wal, enableCompression);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

PrimaryKeyIndex* StorageManager::getPKIndex(table_id_t tableID) {
    KU_ASSERT(tables.contains(tableID));
    KU_ASSERT(tables.at(tableID)->getTableType() == TableType::NODE);
    auto table = ku_dynamic_cast<Table*, NodeTable*>(tables.at(tableID).get());
    return table->getPKIndex();
}

void StorageManager::dropTable(table_id_t tableID) {
    KU_ASSERT(tables.contains(tableID));
    auto tableType = tables.at(tableID)->getTableType();
    switch (tableType) {
    case TableType::NODE: {
        nodesStatisticsAndDeletedIDs->removeTableStatistic(tableID);
    } break;
    case TableType::REL: {
        relsStatistics->removeTableStatistic(tableID);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    tables.erase(tableID);
}

void StorageManager::prepareCommit(transaction::Transaction* transaction) {
    auto localStorage = transaction->getLocalStorage();
    for (auto tableID : localStorage->getTableIDsWithUpdates()) {
        KU_ASSERT(tables.contains(tableID));
        tables.at(tableID)->prepareCommit(transaction, localStorage->getLocalTable(tableID));
    }
    if (nodesStatisticsAndDeletedIDs->hasUpdates()) {
        wal->logTableStatisticsRecord(true /* isNodeTable */);
        nodesStatisticsAndDeletedIDs->writeTablesStatisticsFileForWALRecord(wal->getDirectory());
    }
    if (relsStatistics->hasUpdates()) {
        wal->logTableStatisticsRecord(false /* isNodeTable */);
        relsStatistics->writeTablesStatisticsFileForWALRecord(wal->getDirectory());
    }
}

void StorageManager::prepareRollback(transaction::Transaction* transaction) {
    if (nodesStatisticsAndDeletedIDs->hasUpdates()) {
        wal->logTableStatisticsRecord(true /* isNodeTable */);
    }
    if (relsStatistics->hasUpdates()) {
        wal->logTableStatisticsRecord(false /* isNodeTable */);
    }
    auto localStorage = transaction->getLocalStorage();
    for (auto tableID : localStorage->getTableIDsWithUpdates()) {
        KU_ASSERT(tables.contains(tableID));
        tables.at(tableID)->prepareRollback(localStorage->getLocalTableData(tableID));
    }
}

void StorageManager::checkpointInMemory() {
    for (auto tableID : wal->getUpdatedTables()) {
        KU_ASSERT(tables.contains(tableID));
        tables.at(tableID)->checkpointInMemory();
    }
}

void StorageManager::rollbackInMemory() {
    for (auto tableID : wal->getUpdatedTables()) {
        KU_ASSERT(tables.contains(tableID));
        tables.at(tableID)->rollbackInMemory();
    }
}

} // namespace storage
} // namespace kuzu
