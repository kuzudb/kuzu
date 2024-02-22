#include "storage/storage_manager.h"

#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/wal_replayer.h"
#include "storage/wal_replayer_utils.h"

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

static void setCommonTableIDToRdfRelTable(
    RelTable* relTable, std::vector<RDFGraphCatalogEntry*> rdfEntries) {
    for (auto rdfEntry : rdfEntries) {
        if (rdfEntry->isParent(relTable->getTableID())) {
            std::vector<Column*> columns;
            columns.push_back(relTable->getDirectedTableData(RelDataDirection::FWD)->getColumn(1));
            columns.push_back(relTable->getDirectedTableData(RelDataDirection::BWD)->getColumn(1));
            for (auto& column : columns) {
                ku_dynamic_cast<storage::Column*, storage::InternalIDColumn*>(column)
                    ->setCommonTableID(rdfEntry->getResourceTableID());
            }
        }
    }
}

void StorageManager::loadTables(bool readOnly, const Catalog& catalog) {
    for (auto& nodeTableEntry : catalog.getNodeTableEntries(&DUMMY_READ_TRANSACTION)) {
        KU_ASSERT(!tables.contains(nodeTableEntry->getTableID()));
        tables[nodeTableEntry->getTableID()] = std::make_unique<NodeTable>(dataFH.get(),
            metadataFH.get(), nodeTableEntry, nodesStatisticsAndDeletedIDs.get(), &memoryManager,
            wal, readOnly, enableCompression, vfs);
    }
    auto rdfGraphSchemas = catalog.getRdfGraphEntries(&DUMMY_READ_TRANSACTION);
    for (auto relTableEntry : catalog.getRelTableEntries(&DUMMY_READ_TRANSACTION)) {
        KU_ASSERT(!tables.contains(relTableEntry->getTableID()));
        auto relTable = std::make_unique<RelTable>(dataFH.get(), metadataFH.get(),
            relsStatistics.get(), &memoryManager, relTableEntry, wal, enableCompression);
        setCommonTableIDToRdfRelTable(relTable.get(), rdfGraphSchemas);
        tables[relTableEntry->getTableID()] = std::move(relTable);
    }
}

void StorageManager::createNodeTable(table_id_t tableID, NodeTableCatalogEntry* nodeTableEntry) {
    WALReplayerUtils::createEmptyHashIndexFiles(nodeTableEntry, wal->getDirectory(), vfs);
    tables[tableID] = std::make_unique<NodeTable>(dataFH.get(), metadataFH.get(), nodeTableEntry,
        nodesStatisticsAndDeletedIDs.get(), &memoryManager, wal, false /* readOnly */,
        enableCompression, vfs);
}

void StorageManager::createRelTable(table_id_t tableID, RelTableCatalogEntry* relTableEntry,
    Catalog* catalog, Transaction* transaction) {
    auto relTable = std::make_unique<RelTable>(dataFH.get(), metadataFH.get(), relsStatistics.get(),
        &memoryManager, relTableEntry, wal, enableCompression);
    auto srcTableID = relTableEntry->getSrcTableID();
    auto dstTableID = relTableEntry->getDstTableID();
    auto srcTable = ku_dynamic_cast<Table*, NodeTable*>(tables[srcTableID].get());
    auto dstTable = ku_dynamic_cast<Table*, NodeTable*>(tables[dstTableID].get());
    auto srcPKMetadataDA = srcTable->getColumn(srcTable->getPKColumnID())->getMetadataDA();
    auto dstPKMetadataDA = dstTable->getColumn(dstTable->getPKColumnID())->getMetadataDA();
    relTable->initAdjColumnIfNecessary(
        transaction, srcTableID, dstTableID, srcPKMetadataDA, dstPKMetadataDA);
    setCommonTableIDToRdfRelTable(relTable.get(), catalog->getRdfGraphEntries(transaction));
    tables[tableID] = std::move(relTable);
}

void StorageManager::createTable(table_id_t tableID, Catalog* catalog, Transaction* transaction) {
    auto tableEntry = catalog->getTableCatalogEntry(transaction, tableID);
    switch (tableEntry->getTableType()) {
    case TableType::NODE: {
        createNodeTable(
            tableID, ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(tableEntry));
    } break;
    case TableType::REL: {
        createRelTable(tableID,
            ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(tableEntry), catalog,
            transaction);
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
        WALReplayerUtils::removeHashIndexFile(vfs, tableID, wal->getDirectory());
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
