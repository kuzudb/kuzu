#include "storage/local_storage/local_storage.h"

#include "main/client_context.h"
#include "storage/local_storage/local_node_table.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/local_storage/local_table.h"
#include "storage/storage_manager.h"
#include "storage/store/table.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

LocalTable* LocalStorage::getLocalTable(table_id_t tableID, NotExistAction action) {
    if (!tables.contains(tableID)) {
        switch (action) {
        case NotExistAction::CREATE: {
            const auto table = clientContext.getStorageManager()->getTable(tableID);
            const auto tableEntry = clientContext.getCatalog()->getTableCatalogEntry(
                clientContext.getTransaction(), tableID);
            switch (table->getTableType()) {
            case TableType::NODE: {
                tables[tableID] = std::make_unique<LocalNodeTable>(tableEntry, *table);
            } break;
            case TableType::REL: {
                tables[tableID] = std::make_unique<LocalRelTable>(tableEntry, *table);
            } break;
            default:
                KU_UNREACHABLE;
            }
        } break;
        case NotExistAction::RETURN_NULL: {
            return nullptr;
        }
        default:
            KU_UNREACHABLE;
        }
    }
    return tables.at(tableID).get();
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
            const auto tableEntry = clientContext.getCatalog()->getTableCatalogEntry(
                clientContext.getTransaction(), tableID);
            const auto table = clientContext.getStorageManager()->getTable(tableID);
            table->commit(clientContext.getTransaction(), tableEntry, localTable.get());
        }
    }
}

void LocalStorage::rollback() {
    for (auto& [tableID, localTable] : tables) {
        localTable->clear();
    }
}

uint64_t LocalStorage::getEstimatedMemUsage() const {
    uint64_t totalMemUsage = 0;
    for (const auto& [tableID, localTable] : tables) {
        totalMemUsage += localTable->getEstimatedMemUsage();
    }
    return totalMemUsage;
}

} // namespace storage
} // namespace kuzu
