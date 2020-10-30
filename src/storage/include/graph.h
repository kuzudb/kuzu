#pragma once

#include <vector>

#include "src/storage/include/indexes.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/node_property_store.h"

using namespace std;

namespace graphflow {
namespace common {

class GraphLoader;

} // namespace common
} // namespace graphflow

namespace graphflow {
namespace common {

class GraphLoader;

} // namespace common
} // namespace graphflow

namespace graphflow {
namespace storage {

class Graph {
    friend class graphflow::common::GraphLoader;
    friend class bitsery::Access;

public:
    Graph(const string &directory, uint64_t bufferPoolSize = DEFAULT_BUFFER_POOL_SIZE);

    inline const vector<uint64_t> &getNumNodesPerLabel() const { return numNodesPerLabel; };

    inline const uint64_t getNumNodesForLabel(gfLabel_t label) const {
        return numNodesPerLabel[label];
    };

private:
    Graph() = default;

    template<typename S>
    void serialize(S &s);

    void saveToFile(const string &directory);
    void readFromFile(const string &directory);

private:
    unique_ptr<Catalog> catalog;
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<NodePropertyStore> nodePropertyStore;
    unique_ptr<Indexes> adjListIndexes;
    vector<uint64_t> numNodesPerLabel;
};

} // namespace storage
} // namespace graphflow
