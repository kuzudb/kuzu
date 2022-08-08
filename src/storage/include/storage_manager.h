#pragma once

#include <mutex>

#include "src/storage/store/include/nodes_store.h"
#include "src/storage/store/include/rels_store.h"
#include "src/storage/wal/include/wal.h"

namespace spdlog {
class logger;
}

namespace graphflow {
namespace storage {

class StorageManager {

public:
    StorageManager(
        catalog::Catalog& catalog, BufferManager& bufferManager, bool isInMemoryMode, WAL* wal);

    virtual ~StorageManager();

    inline RelsStore& getRelsStore() const { return *relsStore; }
    inline NodesStore& getNodesStore() const { return *nodesStore; }
    inline Catalog* getCatalog() { return &catalog; }
    void prepareListsToCommitOrRollbackIfNecessary(bool isCommit) {
        nodesStore->prepareUnstructuredPropertyListsToCommitOrRollbackIfNecessary(isCommit);
        // TODO(Semih): When updates to other lists are called, we should call prepareToCommit on
        // other lists as well.
    }
    inline string getDirectory() const { return wal->getDirectory(); }
    inline WAL* getWAL() const { return wal; }

private:
    shared_ptr<spdlog::logger> logger;
    unique_ptr<RelsStore> relsStore;
    unique_ptr<NodesStore> nodesStore;
    Catalog& catalog;
    WAL* wal;
};

} // namespace storage
} // namespace graphflow
