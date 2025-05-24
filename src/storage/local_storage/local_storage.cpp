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
    if (!tables.contains(tableID)) {
        switch (table.getTableType()) {
        case TableType::NODE: {
            auto tableEntry = clientContext.getCatalog()->getTableCatalogEntry(
                clientContext.getTransaction(), table.getTableID());
            tables[tableID] = std::make_unique<LocalNodeTable>(tableEntry, table);
        } break;
        case TableType::REL: {
            // We have to fetch the rel group entry from the catalog to based on the relGroupID.
            auto tableEntry = clientContext.getCatalog()->getTableCatalogEntry(
                clientContext.getTransaction(), table.cast<RelTable>().getRelGroupID());
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

void LocalStorage::commit() {
    for (auto& [tableID, localTable] : tables) {
        if (localTable->getTableType() == TableType::NODE) {
            const auto tableEntry = clientContext.getCatalog()->getTableCatalogEntry(
                clientContext.getTransaction(), tableID);
            const auto table = clientContext.getStorageManager()->getTable(tableID);
            table->commit(clientContext.getTransaction(), tableEntry, localTable.get());
        }
    }
    for (auto& [tableID, localTable] : tables) {
        if (localTable->getTableType() == TableType::REL) {
            const auto table = clientContext.getStorageManager()->getTable(tableID);
            const auto tableEntry = clientContext.getCatalog()->getTableCatalogEntry(
                clientContext.getTransaction(), table->cast<RelTable>().getRelGroupID());
            table->commit(clientContext.getTransaction(), tableEntry, localTable.get());
        }
    }
}

void LocalStorage::rollback() {
    for (auto& [_, localTable] : tables) {
        localTable->clear();
    }
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
