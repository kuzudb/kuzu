#include "storage/local_storage/local_storage.h"

#include "main/client_context.h"
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

LocalTable* LocalStorage::getOrCreateLocalTable(const Table& table) {
    const auto tableID = table.getTableID();
    auto catalog = clientContext.getCatalog();
    auto transaction = clientContext.getTransaction();
    if (!tables.contains(tableID)) {
        switch (table.getTableType()) {
        case TableType::NODE: {
            auto tableEntry = catalog->getTableCatalogEntry(transaction, table.getTableID());
            tables[tableID] = std::make_unique<LocalNodeTable>(tableEntry, table);
        } break;
        case TableType::REL: {
            // We have to fetch the rel group entry from the catalog to based on the relGroupID.
            auto tableEntry =
                catalog->getTableCatalogEntry(transaction, table.cast<RelTable>().getRelGroupID());
            tables[tableID] = std::make_unique<LocalRelTable>(tableEntry, table);
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
    return tables.at(tableID).get();
}

LocalTable* LocalStorage::getLocalTable(table_id_t tableID) const {
    if (tables.contains(tableID)) {
        return tables.at(tableID).get();
    }
    return nullptr;
}

PageAllocator* LocalStorage::addOptimisticAllocator() {
    auto* dataFH = clientContext.getStorageManager()->getDataFH();
    if (dataFH->isInMemoryMode()) {
        return dataFH->getPageManager();
    }
    common::UniqLock lck{mtx};
    optimisticAllocators.emplace_back(
        std::make_unique<OptimisticAllocator>(*dataFH->getPageManager()));
    return optimisticAllocators.back().get();
}

void LocalStorage::commit() {
    auto catalog = clientContext.getCatalog();
    auto transaction = clientContext.getTransaction();
    auto storageManager = clientContext.getStorageManager();
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
    for (auto& [_, localTable] : tables) {
        localTable->clear();
    }
    for (auto& optimisticAllocator : optimisticAllocators) {
        optimisticAllocator->rollback();
    }
    auto* bufferManager = clientContext.getMemoryManager()->getBufferManager();
    clientContext.getStorageManager()->getDataFH()->getPageManager()->clearEvictedBMEntriesIfNeeded(
        bufferManager);
}

uint64_t LocalStorage::getEstimatedMemUsage() const {
    uint64_t totalMemUsage = 0;
    for (const auto& [_, localTable] : tables) {
        totalMemUsage += localTable->getEstimatedMemUsage();
    }
    return totalMemUsage;
}

} // namespace storage
} // namespace kuzu
