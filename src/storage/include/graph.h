#ifndef GRAPHFLOW_STORAGE_GRAPH_H_
#define GRAPHFLOW_STORAGE_GRAPH_H_

#include <vector>

#include "src/storage/include/adjlists_indexes.h"
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
    unique_ptr<AdjListsIndexes> adjListIndexes;
    vector<uint64_t> numNodesPerLabel, numRelsPerLabel;
};

} // namespace storage
} // namespace graphflow

#endif // GRAPHFLOW_STORAGE_GRAPH_H_
