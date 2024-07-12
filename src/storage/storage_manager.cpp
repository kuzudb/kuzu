#include "storage/storage_manager.h"

#include <memory>

#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"
#include "main/database.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/storage_structure/disk_array_collection.h"
#include "storage/store/node_table.h"
#include "storage/wal/wal_record.h"
#include "storage/wal_replayer.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

StorageManager::StorageManager(const std::string& databasePath, bool readOnly,
    const Catalog& catalog, MemoryManager& memoryManager, bool enableCompression,
    VirtualFileSystem* vfs, main::ClientContext* context)
    : databasePath{databasePath}, readOnly{readOnly}, memoryManager{memoryManager},
      enableCompression{enableCompression} {
    dataFH = initFileHandle(StorageUtils::getDataFName(vfs, databasePath), vfs, context);
    metadataFH = initFileHandle(StorageUtils::getMetadataFName(vfs, databasePath), vfs, context);
    // TODO: Assert no wal file exists.
    // TODO: should we disable WAL in readonly mode?
    wal = std::make_unique<WAL>(databasePath, readOnly, *memoryManager.getBufferManager(), vfs,
        context);
    metadataDAC = std::make_unique<DiskArrayCollection>(*metadataFH, DBFileID::newMetadataFileID(),
        memoryManager.getBufferManager(), wal.get());
    nodesStatisticsAndDeletedIDs = std::make_unique<NodesStoreStatsAndDeletedIDs>(databasePath,
        *metadataDAC, memoryManager.getBufferManager(), wal.get(), vfs, context);
    relsStatistics = std::make_unique<RelsStoreStats>(databasePath, *metadataDAC,
        memoryManager.getBufferManager(), wal.get(), vfs, context);
    loadTables(catalog, vfs, context);
}

BMFileHandle* StorageManager::initFileHandle(const std::string& filename, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    return memoryManager.getBufferManager()->getBMFileHandle(filename,
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::VERSIONED_FILE, vfs, context);
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

void StorageManager::loadTables(const Catalog& catalog, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    for (auto& nodeTableEntry : catalog.getNodeTableEntries(&DUMMY_READ_TRANSACTION)) {
        KU_ASSERT(!tables.contains(nodeTableEntry->getTableID()));
        tables[nodeTableEntry->getTableID()] =
            std::make_unique<NodeTable>(this, nodeTableEntry, &memoryManager, vfs, context);
    }
    auto rdfGraphSchemas = catalog.getRdfGraphEntries(&DUMMY_READ_TRANSACTION);
    for (auto relTableEntry : catalog.getRelTableEntries(&DUMMY_READ_TRANSACTION)) {
        KU_ASSERT(!tables.contains(relTableEntry->getTableID()));
        auto relTable = std::make_unique<RelTable>(dataFH, metadataDAC.get(), relsStatistics.get(),
            &memoryManager, relTableEntry, wal.get(), enableCompression);
        setCommonTableIDToRdfRelTable(relTable.get(), rdfGraphSchemas);
        tables[relTableEntry->getTableID()] = std::move(relTable);
    }
}

void StorageManager::recover(main::ClientContext& clientContext) {
    auto vfs = clientContext.getVFSUnsafe();
    auto walFilePath =
        vfs->joinPath(clientContext.getDatabasePath(), StorageConstants::WAL_FILE_SUFFIX);
    if (!vfs->fileOrPathExists(walFilePath, &clientContext)) {
        return;
    }
    try {
        auto* bm = clientContext.getMemoryManager()->getBufferManager();
        auto shadowFH = bm->getBMFileHandle(vfs->joinPath(clientContext.getDatabasePath(),
                                                std::string(StorageConstants::SHADOWING_SUFFIX)),
            FileHandle::O_PERSISTENT_FILE_NO_CREATE,
            BMFileHandle::FileVersionedType::NON_VERSIONED_FILE, vfs, &clientContext);
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
        bm->removeFilePagesFromFrames(*shadowFH);
        StorageUtils::removeCatalogAndStatsWALFiles(clientContext.getDatabasePath(), vfs);
    } catch (std::exception& e) {
        throw Exception(stringFormat("Error during recovery: {}", e.what()));
    }
}

void StorageManager::createNodeTable(table_id_t tableID, NodeTableCatalogEntry* nodeTableEntry,
    main::ClientContext* context) {
    KU_ASSERT(context != nullptr);
    nodesStatisticsAndDeletedIDs->addNodeStatisticsAndDeletedIDs(nodeTableEntry);
    tables[tableID] = std::make_unique<NodeTable>(this, nodeTableEntry, &memoryManager,
        context->getVFSUnsafe(), context);
}

void StorageManager::createRelTable(table_id_t tableID, RelTableCatalogEntry* relTableEntry,
    Catalog* catalog, Transaction* transaction) {
    relsStatistics->addTableStatistic(relTableEntry);
    auto relTable = std::make_unique<RelTable>(dataFH, metadataDAC.get(), relsStatistics.get(),
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
    main::ClientContext* context) {
    KU_ASSERT(context != nullptr);
    auto rdfGraphSchema = ku_dynamic_cast<TableCatalogEntry*, RDFGraphCatalogEntry*>(tableSchema);
    auto resourceTableID = rdfGraphSchema->getResourceTableID();
    auto resourceTableEntry = ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(
        catalog->getTableCatalogEntry(context->getTx(), resourceTableID));
    createNodeTable(resourceTableID, resourceTableEntry, context);
    auto literalTableID = rdfGraphSchema->getLiteralTableID();
    auto literalTableEntry = catalog->getTableCatalogEntry(context->getTx(), literalTableID)
                                 ->ptrCast<NodeTableCatalogEntry>();
    createNodeTable(literalTableID, literalTableEntry, context);
    auto resourceTripleTableID = rdfGraphSchema->getResourceTripleTableID();
    auto resourceTripleTableEntry =
        catalog->getTableCatalogEntry(context->getTx(), resourceTripleTableID)
            ->ptrCast<RelTableCatalogEntry>();
    createRelTable(resourceTripleTableID, resourceTripleTableEntry, catalog, context->getTx());
    auto literalTripleTableID = rdfGraphSchema->getLiteralTripleTableID();
    auto literalTripleTableEntry =
        catalog->getTableCatalogEntry(context->getTx(), literalTripleTableID)
            ->ptrCast<RelTableCatalogEntry>();
    createRelTable(literalTripleTableID, literalTripleTableEntry, catalog, context->getTx());
}

void StorageManager::createTable(table_id_t tableID, Catalog* catalog,
    main::ClientContext* context) {
    auto tableEntry = catalog->getTableCatalogEntry(context->getTx(), tableID);
    switch (tableEntry->getTableType()) {
    case TableType::NODE: {
        createNodeTable(tableID, tableEntry->ptrCast<NodeTableCatalogEntry>(), context);
    } break;
    case TableType::REL: {
        createRelTable(tableID, tableEntry->ptrCast<RelTableCatalogEntry>(), catalog,
            context->getTx());
    } break;
    case TableType::REL_GROUP: {
        createRelTableGroup(tableID, tableEntry->ptrCast<RelGroupCatalogEntry>(), catalog,
            context->getTx());
    } break;
    case TableType::RDF: {
        createRdfGraph(tableID, tableEntry->ptrCast<RDFGraphCatalogEntry>(), catalog, context);
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

void StorageManager::dropTable(table_id_t tableID, VirtualFileSystem* vfs) {
    KU_ASSERT(tables.contains(tableID));
    auto tableType = tables.at(tableID)->getTableType();
    switch (tableType) {
    case TableType::NODE: {
        nodesStatisticsAndDeletedIDs->removeTableStatistic(tableID);
        vfs->removeFileIfExists(
            StorageUtils::getNodeIndexFName(vfs, databasePath, tableID, FileVersionType::ORIGINAL));
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

void StorageManager::prepareCommit(Transaction* transaction, common::VirtualFileSystem* vfs) {
    // Tables which are created but not inserted into may have pending writes
    // which need to be flushed (specifically, the metadata disk array header)
    // TODO(bmwinger): wal->getUpdatedTables isn't the ideal place to store this information
    for (auto tableID : wal->getUpdatedTables()) {
        if (transaction->getLocalStorage()->getLocalTable(tableID) == nullptr) {
            getTable(tableID)->prepareCommit();
        }
    }
    metadataDAC->prepareCommit();
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
    metadataDAC->checkpointInMemory();
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
    metadataDAC->rollbackInMemory();
}

} // namespace storage
} // namespace kuzu
