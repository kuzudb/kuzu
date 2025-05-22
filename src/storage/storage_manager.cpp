#include "storage/storage_manager.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"
#include "main/database.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/page_manager.h"
#include "storage/table/node_table.h"
#include "storage/table/rel_table.h"
#include "storage/wal/wal_replayer.h"
#include "transaction/transaction.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

StorageManager::StorageManager(const std::string& databasePath, bool readOnly,
    MemoryManager& memoryManager, bool enableCompression, VirtualFileSystem* vfs,
    main::ClientContext* context)
    : databasePath{databasePath}, readOnly{readOnly}, dataFH{nullptr}, memoryManager{memoryManager},
      enableCompression{enableCompression} {
    wal = std::make_unique<WAL>(databasePath, readOnly, vfs, context);
    shadowFile = std::make_unique<ShadowFile>(databasePath, readOnly,
        *memoryManager.getBufferManager(), vfs, context);
    inMemory = main::DBConfig::isDBPathInMemory(databasePath);
    initDataFileHandle(vfs, context);
}

StorageManager::~StorageManager() = default;

void StorageManager::initDataFileHandle(VirtualFileSystem* vfs, main::ClientContext* context) {
    if (inMemory) {
        dataFH = memoryManager.getBufferManager()->getFileHandle(databasePath,
            FileHandle::O_PERSISTENT_FILE_IN_MEM, vfs, context);
    } else {
        const auto flag = readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                                     FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS;
        const auto dataFilePath = StorageUtils::getDataFName(vfs, databasePath);
        dataFH = memoryManager.getBufferManager()->getFileHandle(dataFilePath, flag, vfs, context);
    }
    if (dataFH->getNumPages() == 0) {
        // Reserve the first page for the database header.
        dataFH->addNewPage();
    }
}

void StorageManager::recover(main::ClientContext& clientContext) {
    if (main::DBConfig::isDBPathInMemory(clientContext.getDatabasePath())) {
        // In-memory mode. Nothing to recover from.
        return;
    }
    const auto vfs = clientContext.getVFSUnsafe();
    const auto walFilePath =
        vfs->joinPath(clientContext.getDatabasePath(), StorageConstants::WAL_FILE_SUFFIX);
    if (!vfs->fileOrPathExists(walFilePath, &clientContext)) {
        return;
    }
    try {
        // TODO(Guodong): We should first check if there is CHECKPOINT record at the end.
        // If so, we can skip replaying the WAL, instead directly replacing shadow files/pages.
        const auto walReplayer = std::make_unique<WALReplayer>(clientContext);
        walReplayer->replay();
    } catch (std::exception& e) {
        throw Exception(stringFormat("Error during recovery: {}", e.what()));
    }
}

void StorageManager::createNodeTable(NodeTableCatalogEntry* entry) {
    tables[entry->getTableID()] = std::make_unique<NodeTable>(this, entry, &memoryManager);
}

void StorageManager::createRelTable(RelTableCatalogEntry* entry) {
    tables[entry->getTableID()] = std::make_unique<RelTable>(entry, this, &memoryManager);
}

void StorageManager::createRelTableGroup(const RelGroupCatalogEntry* entry,
    const main::ClientContext* context) {
    for (const auto id : entry->getRelTableIDs()) {
        createRelTable(context->getCatalog()
                           ->getTableCatalogEntry(context->getTransaction(), id)
                           ->ptrCast<RelTableCatalogEntry>());
    }
}

void StorageManager::createTable(CatalogEntry* entry, const main::ClientContext* context) {
    std::lock_guard lck{mtx};
    switch (entry->getType()) {
    case CatalogEntryType::NODE_TABLE_ENTRY: {
        createNodeTable(entry->ptrCast<NodeTableCatalogEntry>());
    } break;
    case CatalogEntryType::REL_TABLE_ENTRY: {
        createRelTable(entry->ptrCast<RelTableCatalogEntry>());
    } break;
    case CatalogEntryType::REL_GROUP_ENTRY: {
        createRelTableGroup(entry->ptrCast<RelGroupCatalogEntry>(), context);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

WAL& StorageManager::getWAL() const {
    KU_ASSERT(wal);
    return *wal;
}

ShadowFile& StorageManager::getShadowFile() const {
    KU_ASSERT(shadowFile);
    return *shadowFile;
}

void StorageManager::reclaimDroppedTables(const Catalog& catalog) {
    std::vector<table_id_t> droppedTables;
    for (const auto& [tableID, table] : tables) {
        if (!catalog.containsTable(&DUMMY_CHECKPOINT_TRANSACTION, tableID, true)) {
            table->reclaimStorage(*dataFH);
            droppedTables.push_back(tableID);
        }
    }
    for (auto tableID : droppedTables) {
        tables.erase(tableID);
    }
}

void StorageManager::checkpoint(const Catalog& catalog) {
    std::lock_guard lck{mtx};
    const auto nodeTableEntries = catalog.getNodeTableEntries(&DUMMY_CHECKPOINT_TRANSACTION);
    const auto relTableEntries = catalog.getRelTableEntries(&DUMMY_CHECKPOINT_TRANSACTION);
    for (const auto tableEntry : nodeTableEntries) {
        if (!tables.contains(tableEntry->getTableID())) {
            throw RuntimeException(
                stringFormat("Checkpoint failed: table {} not found in storage manager.",
                    tableEntry->getName()));
        }
        tables.at(tableEntry->getTableID())->checkpoint(tableEntry);
    }
    for (const auto tableEntry : relTableEntries) {
        if (!tables.contains(tableEntry->getTableID())) {
            throw RuntimeException(
                stringFormat("Checkpoint failed: table {} not found in storage manager.",
                    tableEntry->getName()));
        }
        tables.at(tableEntry->getTableID())->checkpoint(tableEntry);
    }
    reclaimDroppedTables(catalog);
}

void StorageManager::finalizeCheckpoint() {
    dataFH->getPageManager()->finalizeCheckpoint();
}

void StorageManager::rollbackCheckpoint(const Catalog& catalog) {
    std::lock_guard lck{mtx};
    const auto nodeTableEntries = catalog.getNodeTableEntries(&DUMMY_CHECKPOINT_TRANSACTION);
    for (const auto tableEntry : nodeTableEntries) {
        KU_ASSERT(tables.contains(tableEntry->getTableID()));
        tables.at(tableEntry->getTableID())->rollbackCheckpoint();
    }
    dataFH->getPageManager()->rollbackCheckpoint();
}

void StorageManager::serialize(const Catalog& catalog, Serializer& ser) {
    std::lock_guard lck{mtx};
    auto nodeTableEntries = catalog.getNodeTableEntries(&DUMMY_CHECKPOINT_TRANSACTION);
    auto relTableEntries = catalog.getRelTableEntries(&DUMMY_CHECKPOINT_TRANSACTION);
    std::sort(nodeTableEntries.begin(), nodeTableEntries.end(),
        [](const auto& a, const auto& b) { return a->getTableID() < b->getTableID(); });
    std::sort(relTableEntries.begin(), relTableEntries.end(),
        [](const auto& a, const auto& b) { return a->getTableID() < b->getTableID(); });
    ser.writeDebuggingInfo("num_node_tables");
    ser.write<uint64_t>(nodeTableEntries.size());
    for (const auto tableEntry : nodeTableEntries) {
        KU_ASSERT(tables.contains(tableEntry->getTableID()));
        ser.writeDebuggingInfo("table_id");
        ser.write<table_id_t>(tableEntry->getTableID());
        tables.at(tableEntry->getTableID())->serialize(ser);
    }
    ser.writeDebuggingInfo("num_rel_tables");
    ser.write<uint64_t>(relTableEntries.size());
    for (const auto tableEntry : relTableEntries) {
        KU_ASSERT(tables.contains(tableEntry->getTableID()));
        ser.writeDebuggingInfo("table_id");
        ser.write<table_id_t>(tableEntry->getTableID());
        tables.at(tableEntry->getTableID())->serialize(ser);
    }
    ser.writeDebuggingInfo("page_manager");
    dataFH->getPageManager()->serialize(ser);
}

void StorageManager::deserialize(const Catalog& catalog, Deserializer& deSer) {
    std::string key;
    deSer.validateDebuggingInfo(key, "num_node_tables");
    uint64_t numNodeTables = 0;
    deSer.deserializeValue<uint64_t>(numNodeTables);
    for (auto i = 0u; i < numNodeTables; i++) {
        deSer.validateDebuggingInfo(key, "table_id");
        table_id_t tableID = INVALID_TABLE_ID;
        deSer.deserializeValue<table_id_t>(tableID);
        if (!catalog.containsTable(&DUMMY_TRANSACTION, tableID)) {
            throw RuntimeException(
                stringFormat("Load table failed: table {} doesn't exist in catalog.", tableID));
        }
        KU_ASSERT(!tables.contains(tableID));
        auto tableEntry = catalog.getTableCatalogEntry(&DUMMY_TRANSACTION, tableID)
                              ->ptrCast<NodeTableCatalogEntry>();
        tables[tableID] = std::make_unique<NodeTable>(this, tableEntry, &memoryManager);
        tables[tableID]->deserialize(tableEntry, deSer);
    }
    deSer.validateDebuggingInfo(key, "num_rel_tables");
    uint64_t numRelTables = 0;
    deSer.deserializeValue<uint64_t>(numRelTables);
    for (auto i = 0u; i < numRelTables; i++) {
        deSer.validateDebuggingInfo(key, "table_id");
        table_id_t tableID = INVALID_TABLE_ID;
        deSer.deserializeValue<table_id_t>(tableID);
        if (!catalog.containsTable(&DUMMY_TRANSACTION, tableID)) {
            throw RuntimeException(
                stringFormat("Load table failed: table {} doesn't exist in catalog.", tableID));
        }
        KU_ASSERT(!tables.contains(tableID));
        auto tableEntry = catalog.getTableCatalogEntry(&DUMMY_TRANSACTION, tableID)
                              ->ptrCast<RelTableCatalogEntry>();
        tables[tableID] = std::make_unique<RelTable>(tableEntry, this, &memoryManager);
        tables[tableID]->deserialize(tableEntry, deSer);
    }
    deSer.validateDebuggingInfo(key, "page_manager");
    dataFH->getPageManager()->deserialize(deSer);
}

} // namespace storage
} // namespace kuzu
