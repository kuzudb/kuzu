#include "storage/local_storage/local_storage.h"

#include "storage/local_storage/local_table.h"
#include "storage/store/column.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

LocalStorage::LocalStorage(MemoryManager* mm) : mm{mm} {}

LocalTableData* LocalStorage::getOrCreateLocalTableData(
    common::table_id_t tableID, const std::vector<std::unique_ptr<Column>>& columns) {
    if (!tables.contains(tableID)) {
        tables.emplace(tableID, std::make_unique<LocalTable>(tableID, TableType::NODE));
    }
    return tables.at(tableID)->getOrCreateLocalTableData(columns, mm);
}

LocalTableData* LocalStorage::getLocalTableData(common::table_id_t tableID) {
    if (!tables.contains(tableID)) {
        return nullptr;
    }
    return tables.at(tableID)->getLocalTableData();
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
