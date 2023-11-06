#pragma once

#include "storage/local_table.h"

namespace kuzu {
namespace storage {
class StorageManager;

// Data structures in LocalStorage are not thread-safe.
// For now, we only support single thread insertions and updates. Once we optimize them with
// multiple threads, LocalStorage and its related data structures should be reworked to be
// thread-safe.
class LocalStorage {
public:
    LocalStorage(storage::StorageManager* storageManager, storage::MemoryManager* mm);

    void scan(common::table_id_t tableID, common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(common::table_id_t tableID, common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void update(common::table_id_t tableID, common::column_id_t columnID,
        common::ValueVector* nodeIDVector, common::ValueVector* propertyVector);
    void update(common::table_id_t tableID, common::column_id_t columnID,
        common::offset_t nodeOffset, common::ValueVector* propertyVector,
        common::sel_t posInPropertyVector);

    void prepareCommit();
    void prepareRollback();

private:
    std::map<common::table_id_t, std::unique_ptr<LocalTable>> tables;
    StorageManager* storageManager;
    storage::MemoryManager* mm;
    bool enableCompression;
};

} // namespace storage
} // namespace kuzu
