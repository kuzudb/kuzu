#pragma once

#include "common/cast.h"
#include "common/enums/table_type.h"
#include "local_node_table.h"
#include "local_rel_table.h"
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
    LocalStorage(storage::MemoryManager* mm) : mm{mm} {}

    void scan(common::table_id_t tableID, common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(common::table_id_t tableID, common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);

    LocalTable* getOrCreateLocalNodeTable(
        common::table_id_t tableID, const std::vector<std::unique_ptr<Column>>& columns);
    LocalRelTableData* getOrCreateLocalRelTableData(common::table_id_t tableID,
        common::RelDataDirection direction, common::ColumnDataFormat dataFormat,
        const std::vector<std::unique_ptr<Column>>& columns);

    std::unordered_set<common::table_id_t> getIDOfTablesWithUpdates();

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
