#include "src/storage/include/store/rels_store.h"

namespace graphflow {
namespace storage {

RelsStore::RelsStore(const Catalog& catalog, BufferManager& bufferManager, const string& directory,
    bool isInMemoryMode, WAL* wal)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")} {
    rels.resize(catalog.getNumRelLabels());
    for (auto label = 0u; label < catalog.getNumRelLabels(); label++) {
        rels[label] =
            make_unique<Rel>(catalog, label, directory, bufferManager, isInMemoryMode, wal);
    }
}

} // namespace storage
} // namespace graphflow
