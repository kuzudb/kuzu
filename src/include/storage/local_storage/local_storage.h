#pragma once

#include <unordered_map>

#include "storage/local_storage/local_table.h"

namespace kuzu {
namespace storage {

class Column;
class MemoryManager;

// Data structures in LocalStorage are not thread-safe.
// For now, we only support single thread insertions and updates. Once we optimize them with
// multiple threads, LocalStorage and its related data structures should be reworked to be
// thread-safe.
class LocalStorage {
public:
    LocalStorage(storage::MemoryManager* mm);

    // This function will create the local table data if not exists.
    LocalTableData* getOrCreateLocalTableData(common::table_id_t tableID,
        const std::vector<std::unique_ptr<Column>>& columns,
        common::TableType tableType = common::TableType::NODE,
        common::ColumnDataFormat dataFormat = common::ColumnDataFormat::REGULAR,
        common::vector_idx_t dataIdx = 0);
    LocalTable* getLocalTable(common::table_id_t tableID);
    // This function will return nullptr if the local table does not exist.
    LocalTableData* getLocalTableData(common::table_id_t tableID, common::vector_idx_t dataIdx = 0);
    std::unordered_set<common::table_id_t> getTableIDsWithUpdates();

private:
    std::unordered_map<common::table_id_t, std::unique_ptr<LocalTable>> tables;
    storage::MemoryManager* mm;
};

} // namespace storage
} // namespace kuzu
