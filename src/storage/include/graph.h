#pragma once

#include <iostream>
#include <vector>

#include "spdlog/spdlog.h"

#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/stores/nodes_store.h"
#include "src/storage/include/stores/rels_store.h"

using namespace std;

namespace graphflow {
namespace loader {

class GraphLoader;

} // namespace loader
} // namespace graphflow

namespace graphflow {
namespace storage {

class Graph {
    friend class graphflow::loader::GraphLoader;
    friend class bitsery::Access;

public:
    Graph(const string& path, uint64_t bufferPoolSize = DEFAULT_BUFFER_POOL_SIZE);

    virtual ~Graph() { spdlog::drop("storage"); };

    inline const Catalog& getCatalog() const { return *catalog; }

    inline const RelsStore& getRelsStore() const { return *relsStore; }

    inline const NodesStore& getNodesStore() const { return *nodesStore; }

    inline const vector<uint64_t>& getNumNodesPerLabel() const { return numNodesPerLabel; };

    inline const uint64_t& getNumNodes(const label_t& label) const {
        return numNodesPerLabel[label];
    };

    inline const string& getPath() const { return path; }

protected:
    Graph() : logger{spdlog::stdout_logger_st("storage")} {};

private:
    template<typename S>
    void serialize(S& s);

    void saveToFile(const string& directory);
    void readFromFile(const string& directory);

private:
    shared_ptr<spdlog::logger> logger;
    const string path;

    unique_ptr<Catalog> catalog;
    unique_ptr<NodesStore> nodesStore;
    unique_ptr<RelsStore> relsStore;
    unique_ptr<BufferManager> bufferManager;
    vector<uint64_t> numNodesPerLabel;
};

} // namespace storage
} // namespace graphflow
