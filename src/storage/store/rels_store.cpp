#include "src/storage/store/include/rels_store.h"

namespace graphflow {
namespace storage {

RelsStore::RelsStore(const Catalog& catalog, const vector<uint64_t>& maxNodeOffsetsPerLabel,
    BufferManager& bufferManager, const string& directory, bool isInMemoryMode, WAL* wal)
    : isInMemoryMode{isInMemoryMode} {
    relTables.resize(catalog.getReadOnlyVersion()->getNumRelLabels());
    for (auto label = 0u; label < catalog.getReadOnlyVersion()->getNumRelLabels(); label++) {
        relTables[label] = make_unique<RelTable>(
            catalog, maxNodeOffsetsPerLabel, label, bufferManager, isInMemoryMode, wal);
    }
}

pair<vector<AdjLists*>, vector<AdjColumn*>> RelsStore::getAdjListsAndColumns(
    const label_t nodeLabel) const {
    vector<AdjLists*> adjListsRetVal;
    for (uint64_t i = 0; i < relTables.size(); ++i) {
        auto adjListsForRel = relTables[i]->getAdjListsForNodeLabel(nodeLabel);
        adjListsRetVal.insert(adjListsRetVal.end(), adjListsForRel.begin(), adjListsForRel.end());
    }
    vector<AdjColumn*> adjColumnsRetVal;
    for (uint64_t i = 0; i < relTables.size(); ++i) {
        auto adjColumnsForRel = relTables[i]->getAdjColumnsForNodeLabel(nodeLabel);
        adjColumnsRetVal.insert(
            adjColumnsRetVal.end(), adjColumnsForRel.begin(), adjColumnsForRel.end());
    }
    return make_pair(adjListsRetVal, adjColumnsRetVal);
}

} // namespace storage
} // namespace graphflow
