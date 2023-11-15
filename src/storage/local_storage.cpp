#include "storage/local_storage.h"

#include "storage/store/column.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

LocalStorage::LocalStorage(MemoryManager* mm) : mm{mm} {}

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

void LocalStorage::insert(table_id_t tableID, ValueVector* nodeIDVector,
    const std::vector<ValueVector*>& propertyVectors) {
    KU_ASSERT(tables.contains(tableID));
    tables.at(tableID)->insert(nodeIDVector, propertyVectors);
}

void LocalStorage::update(table_id_t tableID, ValueVector* nodeIDVector, column_id_t columnID,
    ValueVector* propertyVector) {
    KU_ASSERT(tables.contains(tableID));
    tables.at(tableID)->update(nodeIDVector, columnID, propertyVector);
}

void LocalStorage::delete_(common::table_id_t tableID, common::ValueVector* nodeIDVector) {
    if (!tables.contains(tableID)) {
        return;
    }
    tables.at(tableID)->delete_(nodeIDVector);
}

void LocalStorage::initializeLocalTable(
    table_id_t tableID, const std::vector<std::unique_ptr<Column>>& columns) {
    if (!tables.contains(tableID)) {
        std::vector<std::unique_ptr<LogicalType>> dataTypes;
        for (auto& column : columns) {
            dataTypes.emplace_back(column->getDataType()->copy());
        }
        tables.emplace(tableID, std::make_unique<LocalTable>(tableID, std::move(dataTypes), mm));
    }
}

} // namespace storage
} // namespace kuzu
