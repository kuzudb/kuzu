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
    StorageManager(const catalog::Catalog& catalog, BufferManager& bufferManager, string directory,
        bool isInMemoryMode);

    virtual ~StorageManager();

    inline RelsStore& getRelsStore() const { return *relsStore; }
    inline NodesStore& getNodesStore() const { return *nodesStore; }
    inline WAL& getWAL() const { return *wal; }
    inline void checkpointAndClearWAL() {
        checkpointOrRollbackAndClearWAL(true /* isCheckpoint */);
    }

    inline void rollbackAndClearWAL() {
        checkpointOrRollbackAndClearWAL(false /* rolling back updates */);
    }

private:
    void checkpointOrRollbackAndClearWAL(bool isCheckpoint);
    void recoverIfNecessary();

private:
    shared_ptr<spdlog::logger> logger;
    BufferManager& bufferManager;
    string directory;
    unique_ptr<storage::WAL> wal;
    unique_ptr<NodesStore> nodesStore;
    unique_ptr<RelsStore> relsStore;
    mutex checkpointMtx;
};

} // namespace storage
} // namespace graphflow
