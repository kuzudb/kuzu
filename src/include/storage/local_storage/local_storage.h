#pragma once

#include <shared_mutex>
#include <unordered_map>

#include "common/copy_constructors.h"
#include "storage/local_storage/local_table.h"
#include "storage/optimistic_allocator.h"

namespace kuzu {
namespace main {
class ClientContext;
} // namespace main
namespace storage {
// LocalStorage is thread-safe for concurrent access.
// Multiple threads can safely call getOrCreateLocalTable() simultaneously.
// Internal data structures use fine-grained locking for optimal performance.
class LocalStorage {
public:
    explicit LocalStorage(main::ClientContext& clientContext) : clientContext{clientContext} {}
    DELETE_COPY_AND_MOVE(LocalStorage);

    // Do nothing if the table already exists, otherwise create a new local table.
    // Thread-safe: Multiple threads can call this simultaneously.
    LocalTable* getOrCreateLocalTable(Table& table);
    // Return nullptr if no local table exists.
    // Thread-safe: Read-only access.
    LocalTable* getLocalTable(common::table_id_t tableID) const;

    PageAllocator* addOptimisticAllocator();

    void commit();
    void rollback();

private:
    main::ClientContext& clientContext;

    // Protects concurrent access to tables map
    mutable std::shared_mutex tablesMutex;
    std::unordered_map<common::table_id_t, std::unique_ptr<LocalTable>> tables;

    // Protects concurrent access to optimisticAllocators
    std::mutex allocatorMutex;
    std::vector<std::unique_ptr<OptimisticAllocator>> optimisticAllocators;
};

} // namespace storage
} // namespace kuzu
