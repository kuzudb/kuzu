#pragma once

#include <mutex>

#include "storage/store/nodes_store.h"
#include "storage/store/rels_store.h"
#include "storage/wal/wal.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

class StorageManager {

public:
    StorageManager(catalog::Catalog& catalog, BufferManager& bufferManager,
        MemoryManager& memoryManager, bool isInMemoryMode, WAL* wal);

    ~StorageManager() = default;

    inline RelsStore& getRelsStore() const { return *relsStore; }
    inline NodesStore& getNodesStore() const { return *nodesStore; }
    inline Catalog* getCatalog() { return &catalog; }
    inline void prepareCommitOrRollbackIfNecessary(bool isCommit) {
        nodesStore->prepareCommitOrRollbackIfNecessary(isCommit);
        relsStore->prepareCommitOrRollbackIfNecessary(isCommit);
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
} // namespace kuzu
