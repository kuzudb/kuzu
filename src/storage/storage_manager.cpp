#include "storage/storage_manager.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"
#include "main/database.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"
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
    // TODO: should we disable WAL in readonly mode?
    wal = std::make_unique<WAL>(databasePath, readOnly, *memoryManager.getBufferManager(), vfs,
        context);
    shadowFile = std::make_unique<ShadowFile>(databasePath, readOnly,
        *memoryManager.getBufferManager(), vfs, context);
    dataFH = initFileHandle(StorageUtils::getDataFName(vfs, databasePath), vfs, context);
    metadataFH = initFileHandle(
        StorageUtils::getMetadataFName(vfs, databasePath, FileVersionType::ORIGINAL), vfs, context);
    loadTables(catalog, vfs, context);
}

BMFileHandle* StorageManager::initFileHandle(const std::string& filename, VirtualFileSystem* vfs,
    main::ClientContext* context) const {
    return memoryManager.getBufferManager()->getBMFileHandle(filename,
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        vfs, context);
}

// TODO(Guodong): Rework setting common table ID for rdf rel tables.
static void setCommonTableIDToRdfRelTable(RelTable* relTable,
    std::vector<RDFGraphCatalogEntry*> rdfEntries) {
    for (auto rdfEntry : rdfEntries) {
        if (rdfEntry->isParent(relTable->getTableID())) {
            std::vector<Column*> columns;
            // TODO(Guodong): This is a hack. We should not use constant 2 and should move
            // the setting logic inside RelTableData.
            columns.push_back(relTable->getDirectedTableData(RelDataDirection::FWD)->getColumn(2));
            columns.push_back(relTable->getDirectedTableData(RelDataDirection::BWD)->getColumn(2));
            for (auto& column : columns) {
                ku_dynamic_cast<Column*, InternalIDColumn*>(column)->setCommonTableID(
                    rdfEntry->getResourceTableID());
            }
        }
    }
}

void StorageManager::loadTables(const Catalog& catalog, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    const auto metaFilePath =
        StorageUtils::getMetadataFName(vfs, databasePath, FileVersionType::ORIGINAL);
    if (vfs->fileOrPathExists(metaFilePath, context)) {
        auto metadataFileInfo = vfs->openFile(metaFilePath, O_RDONLY, context);
        if (metadataFileInfo->getFileSize() > 0) {
            Deserializer deSer(std::make_unique<BufferedFileReader>(std::move(metadataFileInfo)));
            std::string key;
            uint64_t numTables;
            deSer.validateDebuggingInfo(key, "num_tables");
            deSer.deserializeValue<uint64_t>(numTables);
            for (auto i = 0u; i < numTables; i++) {
                auto table = Table::loadTable(deSer, catalog, this, &memoryManager, vfs, context);
                tables[table->getTableID()] = std::move(table);
            }
        }
    }

    // TODO(Guodong): Rework setting common table ID for rdf rel tables.
    auto rdfGraphSchemas = catalog.getRdfGraphEntries(&DUMMY_TRANSACTION);
    for (auto relTableEntry : catalog.getRelTableEntries(&DUMMY_TRANSACTION)) {
        KU_ASSERT(tables.contains(relTableEntry->getTableID()));
        auto& relTable = tables.at(relTableEntry->getTableID()).get()->cast<RelTable>();
        setCommonTableIDToRdfRelTable(&relTable, rdfGraphSchemas);
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
        // TODO(Guodong): We should first check if there is CHECKPOINT record at the end.
        // If so, we can skip replaying the WAL, instead directly replacing shadow files/pages.
        auto walReplayer = std::make_unique<WALReplayer>(clientContext);
        walReplayer->replay();
    } catch (std::exception& e) {
        throw Exception(stringFormat("Error during recovery: {}", e.what()));
    }
}

void StorageManager::createNodeTable(table_id_t tableID, NodeTableCatalogEntry* nodeTableEntry,
    main::ClientContext* context) {
    KU_ASSERT(context != nullptr);
    tables[tableID] = std::make_unique<NodeTable>(this, nodeTableEntry, &memoryManager,
        context->getVFSUnsafe(), context);
}

void StorageManager::createRelTable(table_id_t tableID, RelTableCatalogEntry* relTableEntry,
    Catalog* catalog, Transaction* transaction) {
    auto relTable = std::make_unique<RelTable>(relTableEntry, this, &memoryManager);
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
    std::lock_guard lck{mtx};
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
    std::lock_guard lck{mtx};
    KU_ASSERT(tables.contains(tableID));
    KU_ASSERT(tables.at(tableID)->getTableType() == TableType::NODE);
    auto table = ku_dynamic_cast<Table*, NodeTable*>(tables.at(tableID).get());
    return table->getPKIndex();
}

WAL& StorageManager::getWAL() {
    KU_ASSERT(wal);
    return *wal;
}

ShadowFile& StorageManager::getShadowFile() {
    KU_ASSERT(shadowFile);
    return *shadowFile;
}

uint64_t StorageManager::getEstimatedMemoryUsage() {
    std::lock_guard lck{mtx};
    uint64_t totalMemoryUsage = 0;
    for (const auto& [tableID, table] : tables) {
        totalMemoryUsage += table->getEstimatedMemoryUsage();
    }
    return totalMemoryUsage;
}

void StorageManager::checkpoint(main::ClientContext& clientContext) {
    std::lock_guard lck{mtx};
    const auto metadataFileInfo = clientContext.getVFSUnsafe()->openFile(
        StorageUtils::getMetadataFName(clientContext.getVFSUnsafe(), databasePath,
            FileVersionType::WAL_VERSION),
        O_RDWR | O_CREAT, &clientContext);
    const auto writer = std::make_shared<BufferedFileWriter>(*metadataFileInfo);
    Serializer ser(writer);
    const auto nodeTableEntries =
        clientContext.getCatalog()->getNodeTableEntries(&DUMMY_CHECKPOINT_TRANSACTION);
    const auto relTableEntries =
        clientContext.getCatalog()->getRelTableEntries(&DUMMY_CHECKPOINT_TRANSACTION);
    const auto numTables = nodeTableEntries.size() + relTableEntries.size();
    ser.writeDebuggingInfo("num_tables");
    ser.write<uint64_t>(numTables);
    for (const auto tableEntry : nodeTableEntries) {
        if (!tables.contains(tableEntry->getTableID())) {
            throw RuntimeException(
                stringFormat("Checkpoint failed: table {} not found in storage manager.",
                    tableEntry->getName()));
        }
        tables.at(tableEntry->getTableID())->checkpoint(ser);
    }
    for (const auto tableEntry : relTableEntries) {
        if (!tables.contains(tableEntry->getTableID())) {
            throw RuntimeException(
                stringFormat("Checkpoint failed: table {} not found in storage manager.",
                    tableEntry->getName()));
        }
        tables.at(tableEntry->getTableID())->checkpoint(ser);
    }
    writer->flush();
    writer->getFileInfo().syncFile();
    shadowFile->flushAll(clientContext);
}

} // namespace storage
} // namespace kuzu
