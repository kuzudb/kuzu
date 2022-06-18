#pragma once

#include <memory>
#include <vector>

#include "nodes_metadata.h"

#include "src/storage/store/include/node_table.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace storage {

class NodesStore {

public:
    NodesStore(const Catalog& catalog, BufferManager& bufferManager, const string& directory,
        bool isInMemoryMode, WAL* wal);

    inline Column* getNodePropertyColumn(label_t nodeLabel, uint64_t propertyIdx) const {
        return nodeTables[nodeLabel]->getPropertyColumn(propertyIdx);
    }
    inline UnstructuredPropertyLists* getNodeUnstrPropertyLists(label_t nodeLabel) const {
        return nodeTables[nodeLabel]->getUnstrPropertyLists();
    }
    inline HashIndex* getIDIndex(label_t nodeLabel) { return nodeTables[nodeLabel]->getIDIndex(); }

    inline NodesMetadata& getNodesMetadata() { return nodesMetadata; }

    inline NodeTable* getNode(label_t nodeLabel) const { return nodeTables[nodeLabel].get(); }

private:
    vector<unique_ptr<NodeTable>> nodeTables;
    // TODO(Reviewer): Should we make this unique_ptr? We pass reference of this to planMapper.
    // In general, I don't fully understand the rule of thumbs about when to keep a field as
    // a direct object vs a pointer.
    NodesMetadata nodesMetadata;
};

} // namespace storage
} // namespace graphflow
