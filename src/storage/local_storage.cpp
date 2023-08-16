#include "storage/local_storage.h"

#include "storage/storage_manager.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

LocalStorage::LocalStorage(StorageManager* storageManager, MemoryManager* mm)
    : nodesStore{&storageManager->getNodesStore()}, mm{mm} {}

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

void LocalStorage::update(table_id_t tableID, property_id_t propertyID, ValueVector* nodeIDVector,
    ValueVector* propertyVector) {
    if (!tables.contains(tableID)) {
        tables.emplace(tableID, std::make_unique<LocalTable>(nodesStore->getNodeTable(tableID)));
    }
    tables.at(tableID)->update(propertyID, nodeIDVector, propertyVector, mm);
}

void LocalStorage::update(table_id_t tableID, property_id_t propertyID, offset_t nodeOffset,
    ValueVector* propertyVector, sel_t posInPropertyVector) {
    if (!tables.contains(tableID)) {
        tables.emplace(tableID, std::make_unique<LocalTable>(nodesStore->getNodeTable(tableID)));
    }
    tables.at(tableID)->update(propertyID, nodeOffset, propertyVector, posInPropertyVector, mm);
}

void LocalStorage::prepareCommit() {
    for (auto& [_, table] : tables) {
        table->prepareCommit();
    }
    tables.clear();
}

void LocalStorage::prepareRollback() {
    tables.clear();
}

} // namespace storage
} // namespace kuzu
