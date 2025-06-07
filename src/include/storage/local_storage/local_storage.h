#pragma once

#include <unordered_map>

#include "common/copy_constructors.h"
#include "storage/local_storage/local_table.h"

namespace kuzu {
namespace catalog {
class Catalog;
} // namespace catalog
namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {
// Data structures in LocalStorage are not thread-safe.
// For now, we only support single thread insertions and updates. Once we optimize them with
// multiple threads, LocalStorage and its related data structures should be reworked to be
// thread-safe.
class LocalStorage {
public:
    explicit LocalStorage(transaction::Transaction* transaction, catalog::Catalog* catalog,
        StorageManager* storageManager)
        : transaction{transaction}, catalog{catalog}, storageManager{storageManager} {}
    DELETE_COPY_AND_MOVE(LocalStorage);

    // Do nothing if the table already exists, otherwise create a new local table.
    LocalTable* getOrCreateLocalTable(const Table& table);
    // Return nullptr if no local table exists.
    LocalTable* getLocalTable(common::table_id_t tableID) const;

    void commit();
    void rollback();

    uint64_t getEstimatedMemUsage() const;

private:
    transaction::Transaction* transaction;
    catalog::Catalog* catalog;
    StorageManager* storageManager;
    std::unordered_map<common::table_id_t, std::unique_ptr<LocalTable>> tables;
};

} // namespace storage
} // namespace kuzu
