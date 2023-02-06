#include "storage/store/rels_store.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace storage {

RelsStore::RelsStore(
    const Catalog& catalog, BufferManager& bufferManager, MemoryManager& memoryManager, WAL* wal)
    : relsStatistics{wal->getDirectory()} {
    for (auto& tableIDSchema : catalog.getReadOnlyVersion()->getRelTableSchemas()) {
        relTables[tableIDSchema.first] = std::make_unique<RelTable>(
            catalog, tableIDSchema.first, bufferManager, memoryManager, wal);
    }
}

std::pair<std::vector<AdjLists*>, std::vector<AdjColumn*>> RelsStore::getAdjListsAndColumns(
    const table_id_t tableID) const {
    std::vector<AdjLists*> adjListsRetVal;
    for (auto& relTable : relTables) {
        auto adjListsForRel = relTable.second->getAdjListsForNodeTable(tableID);
        adjListsRetVal.insert(adjListsRetVal.end(), adjListsForRel.begin(), adjListsForRel.end());
    }
    std::vector<AdjColumn*> adjColumnsRetVal;
    for (auto& relTable : relTables) {
        auto adjColumnsForRel = relTable.second->getAdjColumnsForNodeTable(tableID);
        adjColumnsRetVal.insert(
            adjColumnsRetVal.end(), adjColumnsForRel.begin(), adjColumnsForRel.end());
    }
    return std::make_pair(adjListsRetVal, adjColumnsRetVal);
}

} // namespace storage
} // namespace kuzu
