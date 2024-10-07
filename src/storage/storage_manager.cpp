#include "storage/storage_manager.h"

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"
#include "main/database.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"
#include "storage/wal_replayer.h"
#include "transaction/transaction.h"

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
    wal = std::make_unique<WAL>(databasePath, readOnly, vfs, context);
    shadowFile = std::make_unique<ShadowFile>(databasePath, readOnly,
        *memoryManager.getBufferManager(), vfs, context);
    dataFH = initFileHandle(StorageUtils::getDataFName(vfs, databasePath), vfs, context);
    metadataFH = initFileHandle(
        StorageUtils::getMetadataFName(vfs, databasePath, FileVersionType::ORIGINAL), vfs, context);
    loadTables(catalog, vfs, context);
}

FileHandle* StorageManager::initFileHandle(const std::string& fileName, VirtualFileSystem* vfs,
    main::ClientContext* context) const {
    if (main::DBConfig::isDBPathInMemory(databasePath)) {
        return memoryManager.getBufferManager()->getFileHandle(fileName,
            FileHandle::O_PERSISTENT_FILE_IN_MEM, vfs, context);
    }
    return memoryManager.getBufferManager()->getFileHandle(fileName,
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        vfs, context);
}

void StorageManager::loadTables(const Catalog& catalog, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    if (main::DBConfig::isDBPathInMemory(databasePath)) {
        return;
    }
    const auto metaFilePath =
        StorageUtils::getMetadataFName(vfs, databasePath, FileVersionType::ORIGINAL);
    if (vfs->fileOrPathExists(metaFilePath, context)) {
        auto metadataFileInfo = vfs->openFile(metaFilePath, FileFlags::READ_ONLY, context);
        if (metadataFileInfo->getFileSize() > 0) {
            Deserializer deSer(std::make_unique<BufferedFileReader>(std::move(metadataFileInfo)));
            std::string key;
            uint64_t numTables = 0;
            deSer.validateDebuggingInfo(key, "num_tables");
            deSer.deserializeValue<uint64_t>(numTables);
            for (auto i = 0u; i < numTables; i++) {
                auto table = Table::loadTable(deSer, catalog, this, &memoryManager, vfs, context);
                tables[table->getTableID()] = std::move(table);
            }
        }
    }
}

void StorageManager::recover(main::ClientContext& clientContext) {
    if (clientContext.getDatabasePath().empty()) {
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

void StorageManager::createNodeTable(table_id_t tableID, NodeTableCatalogEntry* nodeTableEntry,
    main::ClientContext* context) {
    KU_ASSERT(context != nullptr);
    tables[tableID] = std::make_unique<NodeTable>(this, nodeTableEntry, &memoryManager,
        context->getVFSUnsafe(), context);
}

void StorageManager::createRelTable(table_id_t tableID, RelTableCatalogEntry* relTableEntry,
    const Catalog*, Transaction*) {
    tables[tableID] = std::make_unique<RelTable>(relTableEntry, this, &memoryManager);
}

void StorageManager::createRelTableGroup(table_id_t, const RelGroupCatalogEntry* tableSchema,
    const Catalog* catalog, Transaction* transaction) {
    for (const auto relTableID : tableSchema->getRelTableIDs()) {
        createRelTable(relTableID,
            ku_dynamic_cast<RelTableCatalogEntry*>(
                catalog->getTableCatalogEntry(transaction, relTableID)),
            catalog, transaction);
    }
}

void StorageManager::createTable(table_id_t tableID, const Catalog* catalog,
    main::ClientContext* context) {
    std::lock_guard lck{mtx};
    const auto tableEntry = catalog->getTableCatalogEntry(context->getTx(), tableID);
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
    default: {
        KU_UNREACHABLE;
    }
    }
}

PrimaryKeyIndex* StorageManager::getPKIndex(table_id_t tableID) {
    std::lock_guard lck{mtx};
    KU_ASSERT(tables.contains(tableID));
    KU_ASSERT(tables.at(tableID)->getTableType() == TableType::NODE);
    const auto table = ku_dynamic_cast<NodeTable*>(tables.at(tableID).get());
    return table->getPKIndex();
}

WAL& StorageManager::getWAL() const {
    KU_ASSERT(wal);
    return *wal;
}

ShadowFile& StorageManager::getShadowFile() const {
    KU_ASSERT(shadowFile);
    return *shadowFile;
}

void StorageManager::checkpoint(main::ClientContext& clientContext) {
    if (main::DBConfig::isDBPathInMemory(databasePath)) {
        return;
    }
    std::lock_guard lck{mtx};
    const auto metadataFileInfo = clientContext.getVFSUnsafe()->openFile(
        StorageUtils::getMetadataFName(clientContext.getVFSUnsafe(), databasePath,
            FileVersionType::WAL_VERSION),
        FileFlags::READ_ONLY | FileFlags::WRITE | FileFlags::CREATE_IF_NOT_EXISTS, &clientContext);
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
        tables.at(tableEntry->getTableID())->checkpoint(ser, tableEntry);
    }
    for (const auto tableEntry : relTableEntries) {
        if (!tables.contains(tableEntry->getTableID())) {
            throw RuntimeException(
                stringFormat("Checkpoint failed: table {} not found in storage manager.",
                    tableEntry->getName()));
        }
        tables.at(tableEntry->getTableID())->checkpoint(ser, tableEntry);
    }
    writer->flush();
    writer->sync();
    shadowFile->flushAll();
}

StorageManager::~StorageManager() = default;

} // namespace storage
} // namespace kuzu
