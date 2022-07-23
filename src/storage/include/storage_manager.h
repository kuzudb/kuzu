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
        checkpointOrRollbackAndClearWAL(false /* is not recovering */, true /* isCheckpoint */);
    }

    inline void rollbackAndClearWAL() {
        checkpointOrRollbackAndClearWAL(
            false /* is not recovering */, false /* rolling back updates */);
    }

    inline string getDBDirectory() { return directory; }

private:
    void checkpointOrRollbackAndClearWAL(bool isRecovering, bool isCheckpoint);
    void recoverIfNecessary();

private:
    shared_ptr<spdlog::logger> logger;
    BufferManager& bufferManager;
    string directory;
    unique_ptr<storage::WAL> wal;
    unique_ptr<RelsStore> relsStore;
    unique_ptr<NodesStore> nodesStore;
    mutex checkpointMtx;
};

} // namespace storage
} // namespace graphflow
