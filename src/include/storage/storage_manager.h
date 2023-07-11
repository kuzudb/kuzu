#pragma once

#include <mutex>

#include "storage/store/nodes_store.h"
#include "storage/store/rels_store.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace storage {

class StorageManager {
public:
    StorageManager(catalog::Catalog& catalog, MemoryManager& memoryManager, WAL* wal);

    ~StorageManager() = default;

    inline RelsStore& getRelsStore() const { return *relsStore; }
    inline NodesStore& getNodesStore() const { return *nodesStore; }
    inline catalog::Catalog* getCatalog() { return &catalog; }
    inline void prepareCommit() {
        nodesStore->prepareCommit();
        relsStore->prepareCommit();
    }
    inline void prepareRollback() {
        nodesStore->prepareRollback();
        relsStore->prepareRollback();
    }
    inline void checkpointInMemory() {
        nodesStore->checkpointInMemory(wal->updatedNodeTables);
        relsStore->checkpointInMemory(wal->updatedRelTables);
    }
    inline void rollbackInMemory() {
        nodesStore->rollbackInMemory(wal->updatedNodeTables);
        relsStore->rollbackInMemory(wal->updatedRelTables);
    }
    inline std::string getDirectory() const { return wal->getDirectory(); }
    inline WAL* getWAL() const { return wal; }

private:
    std::unique_ptr<BMFileHandle> nodeGroupsDataFH;
    catalog::Catalog& catalog;
    WAL* wal;
    std::unique_ptr<RelsStore> relsStore;
    std::unique_ptr<NodesStore> nodesStore;
};

} // namespace storage
} // namespace kuzu
