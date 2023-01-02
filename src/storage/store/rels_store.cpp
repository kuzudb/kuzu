#include "storage/store/rels_store.h"

namespace kuzu {
namespace storage {

RelsStore::RelsStore(const Catalog& catalog, BufferManager& bufferManager,
    MemoryManager& memoryManager, bool isInMemoryMode, WAL* wal)
    : relsStatistics{wal->getDirectory()}, isInMemoryMode{isInMemoryMode} {
    for (auto& tableIDSchema : catalog.getReadOnlyVersion()->getRelTableSchemas()) {
        relTables[tableIDSchema.first] = make_unique<RelTable>(
            catalog, tableIDSchema.first, bufferManager, memoryManager, isInMemoryMode, wal);
    }
}

pair<vector<AdjLists*>, vector<AdjColumn*>> RelsStore::getAdjListsAndColumns(
    const table_id_t tableID) const {
    vector<AdjLists*> adjListsRetVal;
    for (auto& relTable : relTables) {
        auto adjListsForRel = relTable.second->getAdjListsForNodeTable(tableID);
        adjListsRetVal.insert(adjListsRetVal.end(), adjListsForRel.begin(), adjListsForRel.end());
    }
    vector<AdjColumn*> adjColumnsRetVal;
    for (auto& relTable : relTables) {
        auto adjColumnsForRel = relTable.second->getAdjColumnsForNodeTable(tableID);
        adjColumnsRetVal.insert(
            adjColumnsRetVal.end(), adjColumnsForRel.begin(), adjColumnsForRel.end());
    }
    return make_pair(adjListsRetVal, adjColumnsRetVal);
}

} // namespace storage
} // namespace kuzu
