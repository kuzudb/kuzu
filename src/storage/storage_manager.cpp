#include "storage/storage_manager.h"

#include "catalog/rdf_graph_schema.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/wal_replayer.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

StorageManager::StorageManager(bool readOnly, const Catalog& catalog, MemoryManager& memoryManager,
    WAL* wal, bool enableCompression, VirtualFileSystem* vfs)
    : memoryManager{memoryManager}, wal{wal}, enableCompression{enableCompression}, vfs{vfs} {
    dataFH = memoryManager.getBufferManager()->getBMFileHandle(
        StorageUtils::getDataFName(vfs, wal->getDirectory()),
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::VERSIONED_FILE, vfs);
    metadataFH = memoryManager.getBufferManager()->getBMFileHandle(
        StorageUtils::getMetadataFName(vfs, wal->getDirectory()),
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::VERSIONED_FILE, vfs);
    nodesStatisticsAndDeletedIDs = std::make_unique<NodesStoreStatsAndDeletedIDs>(
        metadataFH.get(), memoryManager.getBufferManager(), wal, vfs);
    relsStatistics = std::make_unique<RelsStoreStats>(
        metadataFH.get(), memoryManager.getBufferManager(), wal, vfs);
    loadTables(readOnly, catalog);
}

static void setCommonTableIDToRdfRelTable(RelTable* relTable, std::vector<TableSchema*> schemas) {
    for (auto schema : schemas) {
        if (schema->isParent(relTable->getTableID())) {
            auto rdfSchema = ku_dynamic_cast<TableSchema*, RdfGraphSchema*>(schema);
            std::vector<Column*> columns;
            columns.push_back(relTable->getDirectedTableData(RelDataDirection::FWD)->getColumn(1));
            columns.push_back(relTable->getDirectedTableData(RelDataDirection::BWD)->getColumn(1));
            for (auto& column : columns) {
                ku_dynamic_cast<storage::Column*, storage::InternalIDColumn*>(column)
                    ->setCommonTableID(rdfSchema->getResourceTableID());
            }
        }
    }
}

void StorageManager::loadTables(bool readOnly, const catalog::Catalog& catalog) {
    for (auto& schema : catalog.getNodeTableSchemas(&DUMMY_READ_TRANSACTION)) {
        KU_ASSERT(!tables.contains(schema->tableID));
        auto nodeTableSchema = ku_dynamic_cast<TableSchema*, NodeTableSchema*>(schema);
        tables[schema->tableID] = std::make_unique<NodeTable>(dataFH.get(), metadataFH.get(),
            nodeTableSchema, nodesStatisticsAndDeletedIDs.get(), &memoryManager, wal, readOnly,
            enableCompression, vfs);
    }
    auto rdfGraphSchemas = catalog.getRdfGraphSchemas(&DUMMY_READ_TRANSACTION);
    for (auto schema : catalog.getRelTableSchemas(&DUMMY_READ_TRANSACTION)) {
        KU_ASSERT(!tables.contains(schema->tableID));
        auto relTableSchema = ku_dynamic_cast<TableSchema*, RelTableSchema*>(schema);
        auto relTable = std::make_unique<RelTable>(dataFH.get(), metadataFH.get(),
            relsStatistics.get(), &memoryManager, relTableSchema, wal, enableCompression);
        setCommonTableIDToRdfRelTable(relTable.get(), rdfGraphSchemas);
        tables[schema->tableID] = std::move(relTable);
    }
}

void StorageManager::createTable(
    common::table_id_t tableID, Catalog* catalog, Transaction* transaction) {
    auto tableSchema = catalog->getTableSchema(transaction, tableID);
    switch (tableSchema->tableType) {
    case TableType::NODE: {
        auto nodeTableSchema = ku_dynamic_cast<TableSchema*, NodeTableSchema*>(tableSchema);
        tables[tableID] = std::make_unique<NodeTable>(dataFH.get(), metadataFH.get(),
            nodeTableSchema, nodesStatisticsAndDeletedIDs.get(), &memoryManager, wal,
            false /* readOnly */, enableCompression, vfs);
    } break;
    case TableType::REL: {
        auto relTableSchema = ku_dynamic_cast<TableSchema*, RelTableSchema*>(tableSchema);
        auto relTable = std::make_unique<RelTable>(dataFH.get(), metadataFH.get(),
            relsStatistics.get(), &memoryManager, relTableSchema, wal, enableCompression);
        setCommonTableIDToRdfRelTable(relTable.get(), catalog->getRdfGraphSchemas(transaction));
        tables[tableID] = std::move(relTable);
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
