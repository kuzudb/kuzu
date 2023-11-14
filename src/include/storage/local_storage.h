#pragma once

#include "storage/local_table.h"

namespace kuzu {
namespace storage {

class Column;

// Data structures in LocalStorage are not thread-safe.
// For now, we only support single thread insertions and updates. Once we optimize them with
// multiple threads, LocalStorage and its related data structures should be reworked to be
// thread-safe.
class LocalStorage {
public:
    LocalStorage(storage::MemoryManager* mm);

    void scan(common::table_id_t tableID, common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(common::table_id_t tableID, common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);

    // Note: `initializeLocalTable` should be called before `insert` and `update`.
    void initializeLocalTable(
        common::table_id_t tableID, const std::vector<std::unique_ptr<Column>>& columns);
    void insert(common::table_id_t tableID, common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(common::table_id_t tableID, common::ValueVector* nodeIDVector,
        common::column_id_t columnID, common::ValueVector* propertyVector);
    void delete_(common::table_id_t tableID, common::ValueVector* nodeIDVector);

    inline std::unordered_set<common::table_id_t> getTableIDsWithUpdates() {
        std::unordered_set<common::table_id_t> tableSetToUpdate;
        for (auto& [tableID, _] : tables) {
            tableSetToUpdate.insert(tableID);
        }
        return tableSetToUpdate;
    }
    inline LocalTable* getLocalTable(common::table_id_t tableID) {
        KU_ASSERT(tables.contains(tableID));
        return tables.at(tableID).get();
    }

private:
    std::map<common::table_id_t, std::unique_ptr<LocalTable>> tables;
    storage::MemoryManager* mm;
};

} // namespace storage
} // namespace kuzu
