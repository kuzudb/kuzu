#include "src/storage/store/include/rels_store.h"

namespace graphflow {
namespace storage {

RelsStore::RelsStore(const Catalog& catalog, const vector<uint64_t>& maxNodeOffsetsPerLabel,
    BufferManager& bufferManager, const string& directory, bool isInMemoryMode, WAL* wal) {
    relTables.resize(catalog.getNumRelLabels());
    for (auto label = 0u; label < catalog.getNumRelLabels(); label++) {
        relTables[label] = make_unique<RelTable>(
            catalog, maxNodeOffsetsPerLabel, label, directory, bufferManager, isInMemoryMode, wal);
    }
}

} // namespace storage
} // namespace graphflow
