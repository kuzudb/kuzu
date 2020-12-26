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

    inline virtual BaseColumn* getNodePropertyColumn(
        const label_t& nodeLabel, const string& propertyName) {
        auto propertyIdx = catalog->getIdxForNodeLabelPropertyName(nodeLabel, propertyName);
        return nodesStore->getNodePropertyColumn(nodeLabel, propertyIdx);
    }

    inline BaseColumn* getRelPropertyColumn(
        const label_t& relLabel, const label_t& nodeLabel, const string& propertyName) {
        auto propertyIdx = catalog->getIdxForRelLabelPropertyName(relLabel, propertyName);
        return relsStore->getRelPropertyColumn(relLabel, nodeLabel, propertyIdx);
    }

    inline AdjColumn* getAdjColumn(
        const Direction& direction, const label_t& nodeLabel, const label_t& relLabel) {
        return relsStore->getAdjColumn(direction, nodeLabel, relLabel);
    }

protected:
    Graph() = default;

private:
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
