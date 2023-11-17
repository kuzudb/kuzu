#include "storage/local_storage/local_storage.h"

#include "storage/local_storage/local_table.h"
#include "storage/store/column.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

LocalStorage::LocalStorage(MemoryManager* mm) : mm{mm} {}

LocalTableData* LocalStorage::getOrCreateLocalTableData(table_id_t tableID,
    const std::vector<std::unique_ptr<Column>>& columns, TableType tableType,
    ColumnDataFormat dataFormat, vector_idx_t dataIdx) {
    if (!tables.contains(tableID)) {
        tables[tableID] = std::make_unique<LocalTable>(tableID, tableType);
    }
    return tables.at(tableID)->getOrCreateLocalTableData(columns, mm, dataFormat, dataIdx);
}

LocalTable* LocalStorage::getLocalTable(table_id_t tableID) {
    if (!tables.contains(tableID)) {
        return nullptr;
    }
    return tables.at(tableID).get();
}

LocalTableData* LocalStorage::getLocalTableData(table_id_t tableID, vector_idx_t dataIdx) {
    if (!tables.contains(tableID)) {
        return nullptr;
    }
    return tables.at(tableID)->getLocalTableData(dataIdx);
}

std::unordered_set<table_id_t> LocalStorage::getTableIDsWithUpdates() {
    std::unordered_set<table_id_t> tableSetToUpdate;
    for (auto& [tableID, _] : tables) {
        tableSetToUpdate.insert(tableID);
    }
    return tableSetToUpdate;
}

} // namespace storage
} // namespace kuzu
