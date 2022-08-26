#include "src/storage/store/include/rels_store.h"

namespace graphflow {
namespace storage {

RelsStore::RelsStore(const Catalog& catalog, const vector<uint64_t>& maxNodeOffsetsPerTable,
    BufferManager& bufferManager, bool isInMemoryMode, WAL* wal)
    : relsStatistics{wal->getDirectory()}, isInMemoryMode{isInMemoryMode} {
    relTables.resize(catalog.getReadOnlyVersion()->getNumRelTables());
    for (auto tableID = 0u; tableID < catalog.getReadOnlyVersion()->getNumRelTables(); tableID++) {
        relTables[tableID] = make_unique<RelTable>(
            catalog, maxNodeOffsetsPerTable, tableID, bufferManager, isInMemoryMode, wal);
    }
}

pair<vector<AdjLists*>, vector<AdjColumn*>> RelsStore::getAdjListsAndColumns(
    const table_id_t tableID) const {
    vector<AdjLists*> adjListsRetVal;
    for (uint64_t i = 0; i < relTables.size(); ++i) {
        auto adjListsForRel = relTables[i]->getAdjListsForNodeTable(tableID);
        adjListsRetVal.insert(adjListsRetVal.end(), adjListsForRel.begin(), adjListsForRel.end());
    }
    vector<AdjColumn*> adjColumnsRetVal;
    for (uint64_t i = 0; i < relTables.size(); ++i) {
        auto adjColumnsForRel = relTables[i]->getAdjColumnsForNodeTable(tableID);
        adjColumnsRetVal.insert(
            adjColumnsRetVal.end(), adjColumnsForRel.begin(), adjColumnsForRel.end());
    }
    return make_pair(adjListsRetVal, adjColumnsRetVal);
}

} // namespace storage
} // namespace graphflow
