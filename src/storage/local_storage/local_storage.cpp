#include "storage/local_storage/local_storage.h"

#include "storage/local_storage/local_node_table.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/local_storage/local_table.h"
#include "storage/storage_manager.h"
#include "storage/table/rel_table.h"
#include "storage/table/table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

LocalTable* LocalStorage::getOrCreateLocalTable(Table& table) {
    const auto tableID = table.getTableID();

    // Fast path: Check if table exists with read lock
    {
        std::shared_lock<std::shared_mutex> readLock(tablesMutex);
        if (auto it = tables.find(tableID); it != tables.end()) {
            return it->second.get();
        }
    }

    // Slow path: Create table with write lock
    std::unique_lock<std::shared_mutex> writeLock(tablesMutex);

    // Double-check: Another thread might have created it
    if (auto it = tables.find(tableID); it != tables.end()) {
        return it->second.get();
    }

    // Create new local table
    auto catalog = catalog::Catalog::Get(clientContext);
    auto transaction = transaction::Transaction::Get(clientContext);
    auto& mm = *MemoryManager::Get(clientContext);

    std::unique_ptr<LocalTable> localTable;
    switch (table.getTableType()) {
    case TableType::NODE: {
        auto tableEntry = catalog->getTableCatalogEntry(transaction, table.getTableID());
        localTable = std::make_unique<LocalNodeTable>(tableEntry, table, mm);
    } break;
    case TableType::REL: {
        // We have to fetch the rel group entry from the catalog to based on the relGroupID.
        auto tableEntry =
            catalog->getTableCatalogEntry(transaction, table.cast<RelTable>().getRelGroupID());
        localTable = std::make_unique<LocalRelTable>(tableEntry, table, mm);
    } break;
    default:
        KU_UNREACHABLE;
    }

    auto* result = localTable.get();
    tables.emplace(tableID, std::move(localTable));
    return result;
}

LocalTable* LocalStorage::getLocalTable(table_id_t tableID) const {
    std::shared_lock<std::shared_mutex> readLock(tablesMutex);
    if (auto it = tables.find(tableID); it != tables.end()) {
        return it->second.get();
    }
    return nullptr;
}

PageAllocator* LocalStorage::addOptimisticAllocator() {
    auto* dataFH = StorageManager::Get(clientContext)->getDataFH();
    if (dataFH->isInMemoryMode()) {
        return dataFH->getPageManager();
    }
    std::lock_guard<std::mutex> lock(allocatorMutex);
    optimisticAllocators.emplace_back(
        std::make_unique<OptimisticAllocator>(*dataFH->getPageManager()));
    return optimisticAllocators.back().get();
}

void LocalStorage::commit() {
    auto catalog = catalog::Catalog::Get(clientContext);
    auto transaction = transaction::Transaction::Get(clientContext);
    auto storageManager = StorageManager::Get(clientContext);
    for (auto& [tableID, localTable] : tables) {
        if (localTable->getTableType() == TableType::NODE) {
            const auto tableEntry = catalog->getTableCatalogEntry(transaction, tableID);
            const auto table = storageManager->getTable(tableID);
            table->commit(&clientContext, tableEntry, localTable.get());
        }
    }
    for (auto& [tableID, localTable] : tables) {
        if (localTable->getTableType() == TableType::REL) {
            const auto table = storageManager->getTable(tableID);
            const auto tableEntry =
                catalog->getTableCatalogEntry(transaction, table->cast<RelTable>().getRelGroupID());
            table->commit(&clientContext, tableEntry, localTable.get());
        }
    }
    for (auto& optimisticAllocator : optimisticAllocators) {
        optimisticAllocator->commit();
    }
}

void LocalStorage::rollback() {
    auto mm = MemoryManager::Get(clientContext);
    for (auto& [_, localTable] : tables) {
        localTable->clear(*mm);
    }
    for (auto& optimisticAllocator : optimisticAllocators) {
        optimisticAllocator->rollback();
    }
    auto* bufferManager = mm->getBufferManager();
    PageManager::Get(clientContext)->clearEvictedBMEntriesIfNeeded(bufferManager);
}

} // namespace storage
} // namespace kuzu
