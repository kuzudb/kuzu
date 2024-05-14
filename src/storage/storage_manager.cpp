#include "storage/storage_manager.h"

#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "common/file_system/virtual_file_system.h"
#include "main/database.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/store/node_table.h"
#include "storage/wal_replayer.h"
#include "storage/wal_replayer_utils.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

StorageManager::StorageManager(const std::string& databasePath, bool readOnly,
    const Catalog& catalog, MemoryManager& memoryManager, bool enableCompression,
    VirtualFileSystem* vfs)
    : databasePath{databasePath}, readOnly{readOnly}, memoryManager{memoryManager},
      enableCompression{enableCompression}, vfs{vfs} {
    dataFH = initFileHandle(StorageUtils::getDataFName(vfs, databasePath));
    metadataFH = initFileHandle(StorageUtils::getMetadataFName(vfs, databasePath));
    // TODO: Assert no wal file exists.
    wal = std::make_unique<WAL>(databasePath, readOnly, *memoryManager.getBufferManager(), vfs);
    nodesStatisticsAndDeletedIDs = std::make_unique<NodesStoreStatsAndDeletedIDs>(databasePath,
        metadataFH.get(), memoryManager.getBufferManager(), wal.get(), vfs);
    relsStatistics = std::make_unique<RelsStoreStats>(databasePath, metadataFH.get(),
        memoryManager.getBufferManager(), wal.get(), vfs);
    loadTables(catalog);
}

std::unique_ptr<BMFileHandle> StorageManager::initFileHandle(const std::string& filename) {
    return memoryManager.getBufferManager()->getBMFileHandle(filename,
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::VERSIONED_FILE, vfs);
}

static void setCommonTableIDToRdfRelTable(RelTable* relTable,
    std::vector<RDFGraphCatalogEntry*> rdfEntries) {
    for (auto rdfEntry : rdfEntries) {
        if (rdfEntry->isParent(relTable->getTableID())) {
            std::vector<Column*> columns;
            // TODO(Guodong): This is a hack. We should not use constant 2 and should move the
            // setting logic inside RelTableData.
            columns.push_back(relTable->getDirectedTableData(RelDataDirection::FWD)->getColumn(2));
            columns.push_back(relTable->getDirectedTableData(RelDataDirection::BWD)->getColumn(2));
            for (auto& column : columns) {
                ku_dynamic_cast<storage::Column*, storage::InternalIDColumn*>(column)
                    ->setCommonTableID(rdfEntry->getResourceTableID());
            }
        }
    }
}

void StorageManager::loadTables(const Catalog& catalog) {
    for (auto& nodeTableEntry : catalog.getNodeTableEntries(&DUMMY_READ_TRANSACTION)) {
        KU_ASSERT(!tables.contains(nodeTableEntry->getTableID()));
        tables[nodeTableEntry->getTableID()] =
            std::make_unique<NodeTable>(this, nodeTableEntry, &memoryManager, vfs);
    }
    auto rdfGraphSchemas = catalog.getRdfGraphEntries(&DUMMY_READ_TRANSACTION);
    for (auto relTableEntry : catalog.getRelTableEntries(&DUMMY_READ_TRANSACTION)) {
        KU_ASSERT(!tables.contains(relTableEntry->getTableID()));
        auto relTable = std::make_unique<RelTable>(dataFH.get(), metadataFH.get(),
            relsStatistics.get(), &memoryManager, relTableEntry, wal.get(), enableCompression);
        setCommonTableIDToRdfRelTable(relTable.get(), rdfGraphSchemas);
        tables[relTableEntry->getTableID()] = std::move(relTable);
    }
}

void StorageManager::recover(main::ClientContext& clientContext) {
    auto vfs = clientContext.getVFSUnsafe();
    auto walFilePath =
        vfs->joinPath(clientContext.getDatabasePath(), StorageConstants::WAL_FILE_SUFFIX);
    if (!vfs->fileOrPathExists(walFilePath)) {
        return;
    }
    try {
        auto shadowFH = clientContext.getMemoryManager()->getBufferManager()->getBMFileHandle(
            vfs->joinPath(clientContext.getDatabasePath(),
                std::string(StorageConstants::SHADOWING_SUFFIX)),
            FileHandle::O_PERSISTENT_FILE_NO_CREATE,
            BMFileHandle::FileVersionedType::NON_VERSIONED_FILE, vfs);
        auto walReplayer = std::make_unique<WALReplayer>(clientContext, *shadowFH,
            WALReplayMode::RECOVERY_CHECKPOINT);
        walReplayer->replay();
        // Truncate .wal and .shadow to empty. Remove catalog and stats wal files.
        auto walFileInfo = vfs->openFile(walFilePath, O_RDWR);
        if (walFileInfo->getFileSize() > 0) {
            walFileInfo->truncate(0);
        }
        if (shadowFH->getFileInfo()->getFileSize() > 0) {
            shadowFH->getFileInfo()->truncate(0);
        }
        StorageUtils::removeCatalogAndStatsWALFiles(clientContext.getDatabasePath(), vfs);
    } catch (std::exception& e) {
        throw Exception(stringFormat("Error during recovery: {}", e.what()));
    }
}

void StorageManager::createNodeTable(table_id_t tableID, NodeTableCatalogEntry* nodeTableEntry) {
    nodesStatisticsAndDeletedIDs->addNodeStatisticsAndDeletedIDs(nodeTableEntry);
    WALReplayerUtils::createEmptyHashIndexFiles(nodeTableEntry, databasePath, vfs);
    tables[tableID] = std::make_unique<NodeTable>(this, nodeTableEntry, &memoryManager, vfs);
}

void StorageManager::createRelTable(table_id_t tableID, RelTableCatalogEntry* relTableEntry,
    Catalog* catalog, Transaction* transaction) {
    relsStatistics->addTableStatistic(relTableEntry);
    auto relTable = std::make_unique<RelTable>(dataFH.get(), metadataFH.get(), relsStatistics.get(),
        &memoryManager, relTableEntry, wal.get(), enableCompression);
    setCommonTableIDToRdfRelTable(relTable.get(), catalog->getRdfGraphEntries(transaction));
    tables[tableID] = std::move(relTable);
}

void StorageManager::createRelTableGroup(table_id_t, RelGroupCatalogEntry* tableSchema,
    Catalog* catalog, Transaction* transaction) {
    for (auto relTableID : tableSchema->getRelTableIDs()) {
        createRelTable(relTableID,
            ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(
                catalog->getTableCatalogEntry(transaction, relTableID)),
            catalog, transaction);
    }
}

void StorageManager::createRdfGraph(table_id_t, RDFGraphCatalogEntry* tableSchema, Catalog* catalog,
    Transaction* transaction) {
    auto rdfGraphSchema = ku_dynamic_cast<TableCatalogEntry*, RDFGraphCatalogEntry*>(tableSchema);
    auto resourceTableID = rdfGraphSchema->getResourceTableID();
    auto resourceTableEntry = ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(
        catalog->getTableCatalogEntry(transaction, resourceTableID));
    createNodeTable(resourceTableID, resourceTableEntry);
    auto literalTableID = rdfGraphSchema->getLiteralTableID();
    auto literalTableEntry = ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(
        catalog->getTableCatalogEntry(transaction, literalTableID));
    createNodeTable(literalTableID, literalTableEntry);
    auto resourceTripleTableID = rdfGraphSchema->getResourceTripleTableID();
    auto resourceTripleTableEntry = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(
        catalog->getTableCatalogEntry(transaction, resourceTripleTableID));
    createRelTable(resourceTripleTableID, resourceTripleTableEntry, catalog, transaction);
    auto literalTripleTableID = rdfGraphSchema->getLiteralTripleTableID();
    auto literalTripleTableEntry = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(
        catalog->getTableCatalogEntry(transaction, literalTripleTableID));
    createRelTable(literalTripleTableID, literalTripleTableEntry, catalog, transaction);
}

void StorageManager::createTable(table_id_t tableID, Catalog* catalog, Transaction* transaction) {
    auto tableEntry = catalog->getTableCatalogEntry(transaction, tableID);
    switch (tableEntry->getTableType()) {
    case TableType::NODE: {
        createNodeTable(tableID,
            ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(tableEntry));
    } break;
    case TableType::REL: {
        createRelTable(tableID,
            ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(tableEntry), catalog,
            transaction);
    } break;
    case TableType::REL_GROUP: {
        createRelTableGroup(tableID,
            ku_dynamic_cast<TableCatalogEntry*, RelGroupCatalogEntry*>(tableEntry), catalog,
            transaction);
    } break;
    case TableType::RDF: {
        createRdfGraph(tableID,
            ku_dynamic_cast<TableCatalogEntry*, RDFGraphCatalogEntry*>(tableEntry), catalog,
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

WAL& StorageManager::getWAL() {
    KU_ASSERT(wal);
    return *wal;
}

void StorageManager::dropTable(table_id_t tableID) {
    KU_ASSERT(tables.contains(tableID));
    auto tableType = tables.at(tableID)->getTableType();
    switch (tableType) {
    case TableType::NODE: {
        nodesStatisticsAndDeletedIDs->removeTableStatistic(tableID);
        WALReplayerUtils::removeHashIndexFile(vfs, tableID, databasePath);
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

void StorageManager::prepareCommit(Transaction* transaction) {
    // Tables which are created but not inserted into may have pending writes
    // which need to be flushed (specifically, the metadata disk array header)
    // TODO(bmwinger): wal->getUpdatedTables isn't the ideal place to store this information
    for (auto tableID : wal->getUpdatedTables()) {
        if (transaction->getLocalStorage()->getLocalTable(tableID) == nullptr) {
            getTable(tableID)->prepareCommit();
        }
    }
    if (nodesStatisticsAndDeletedIDs->hasUpdates()) {
        nodesStatisticsAndDeletedIDs->writeTablesStatisticsFileForWALRecord(databasePath, vfs);
        wal->logTableStatisticsRecord(TableType::NODE);
    }
    if (relsStatistics->hasUpdates()) {
        relsStatistics->writeTablesStatisticsFileForWALRecord(databasePath, vfs);
        wal->logTableStatisticsRecord(TableType::REL);
    }
}

void StorageManager::prepareRollback() {
    if (nodesStatisticsAndDeletedIDs->hasUpdates()) {
        wal->logTableStatisticsRecord(TableType::NODE);
    }
    if (relsStatistics->hasUpdates()) {
        wal->logTableStatisticsRecord(TableType::REL);
    }
}

void StorageManager::checkpointInMemory() {
    for (auto tableID : wal->getUpdatedTables()) {
        KU_ASSERT(tables.contains(tableID));
        tables.at(tableID)->checkpointInMemory();
    }
    if (nodesStatisticsAndDeletedIDs->hasUpdates()) {
        nodesStatisticsAndDeletedIDs->checkpointInMemoryIfNecessary();
    }
    if (relsStatistics->hasUpdates()) {
        relsStatistics->checkpointInMemoryIfNecessary();
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
