#pragma once

#include <vector>

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
    Graph(const string& directory, uint64_t bufferPoolSize = DEFAULT_BUFFER_POOL_SIZE);

    inline const vector<uint64_t>& getNumNodesPerLabel() const { return numNodesPerLabel; };

    inline const uint64_t getNumNodesForLabel(label_t label) const {
        return numNodesPerLabel[label];
    };

private:
    Graph() = default;

    template<typename S>
    void serialize(S& s);

    void saveToFile(const string& directory);
    void readFromFile(const string& directory);

private:
    unique_ptr<Catalog> catalog;
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<NodesStore> nodesStore;
    unique_ptr<RelsStore> relsStore;
    vector<uint64_t> numNodesPerLabel;
};

} // namespace storage
} // namespace graphflow
