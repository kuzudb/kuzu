#pragma once

#include "src/storage/include/store/nodes_store.h"
#include "src/storage/include/store/rels_store.h"
#include "src/storage/include/wal/wal.h"

using namespace std;

namespace spdlog {
class logger;
}

namespace graphflow {
namespace loader {

class GraphLoader;
class RelsLoader;

} // namespace loader
} // namespace graphflow

namespace graphflow {
namespace storage {

class StorageManager {
    friend class graphflow::loader::GraphLoader;
    friend class graphflow::loader::RelsLoader;

public:
    StorageManager(const catalog::Catalog& catalog, BufferManager& bufferManager, string directory,
        bool isInMemoryMode);

    virtual ~StorageManager();

    inline RelsStore& getRelsStore() const { return *relsStore; }
    inline NodesStore& getNodesStore() const { return *nodesStore; }

private:
    shared_ptr<spdlog::logger> logger;
    string directory;
    bool isInMemoryMode;
    unique_ptr<storage::WAL> wal;
    unique_ptr<NodesStore> nodesStore;
    unique_ptr<RelsStore> relsStore;
};

} // namespace storage
} // namespace graphflow
