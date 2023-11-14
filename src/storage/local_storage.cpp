#include "storage/local_storage.h"

#include "storage/store/column.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void LocalStorage::scan(table_id_t tableID, ValueVector* nodeIDVector,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    if (!tables.contains(tableID)) {
        return;
    }
    tables.at(tableID)->scan(nodeIDVector, columnIDs, outputVectors);
}

void LocalStorage::lookup(table_id_t tableID, ValueVector* nodeIDVector,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    if (!tables.contains(tableID)) {
        return;
    }
    tables.at(tableID)->lookup(nodeIDVector, columnIDs, outputVectors);
}

std::unordered_set<common::table_id_t> LocalStorage::getIDOfTablesWithUpdates() {
    std::unordered_set<common::table_id_t> tableSetToUpdate;
    for (auto& [tableID, _] : tables) {
        tableSetToUpdate.insert(tableID);
    }
    return tableSetToUpdate;
}

LocalTable* LocalStorage::getOrCreateLocalNodeTable(
    table_id_t tableID, const std::vector<std::unique_ptr<Column>>& columns) {
    if (!tables.contains(tableID)) {
        std::vector<std::unique_ptr<LogicalType>> dataTypes;
        for (auto& column : columns) {
            dataTypes.emplace_back(column->getDataType().copy());
        }
        tables.emplace(
            tableID, std::make_unique<LocalNodeTable>(tableID, std::move(dataTypes), mm));
    }
    return tables.at(tableID).get();
}

LocalRelTableData* LocalStorage::getOrCreateLocalRelTableData(table_id_t tableID,
    RelDataDirection direction, ColumnDataFormat dataFormat,
    const std::vector<std::unique_ptr<Column>>& columns) {
    if (!tables.contains(tableID)) {
        std::vector<std::unique_ptr<LogicalType>> dataTypes;
        for (auto& column : columns) {
            dataTypes.emplace_back(column->getDataType().copy());
        }
        tables.emplace(tableID, std::make_unique<LocalRelTable>(tableID, std::move(dataTypes), mm));
    }
    return common::ku_dynamic_cast<LocalTable*, LocalRelTable*>(tables.at(tableID).get())
        ->getOrCreateRelTableData(direction, dataFormat);
}

} // namespace storage
} // namespace kuzu
