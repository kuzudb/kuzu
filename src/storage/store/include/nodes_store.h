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
    NodesStore(const Catalog& catalog, BufferManager& bufferManager, bool isInMemoryMode, WAL* wal);

    inline Column* getNodePropertyColumn(label_t nodeLabel, uint64_t propertyIdx) const {
        return nodeTables[nodeLabel]->getPropertyColumn(propertyIdx);
    }
    inline UnstructuredPropertyLists* getNodeUnstrPropertyLists(label_t nodeLabel) const {
        return nodeTables[nodeLabel]->getUnstrPropertyLists();
    }
    inline HashIndex* getIDIndex(label_t nodeLabel) { return nodeTables[nodeLabel]->getIDIndex(); }

    inline NodesMetadata& getNodesMetadata() { return nodesMetadata; }

    // TODO: rename to getNodeTable
    inline NodeTable* getNode(label_t nodeLabel) const { return nodeTables[nodeLabel].get(); }

    // Since ddl statements are wrapped in a single auto-committed transaction, we don't need to
    // maintain a write-only version of nodeTables. We just need to add the actual nodeTable to
    // nodeStore when checkpointing and not in recovery mode.
    inline void createNodeTable(
        label_t labelID, BufferManager* bufferManager, WAL* wal, Catalog* catalog) {
        nodeTables.push_back(make_unique<NodeTable>(&nodesMetadata, *bufferManager, isInMemoryMode,
            wal, catalog->getReadOnlyVersion()->getNodeLabel(labelID)));
    }

private:
    vector<unique_ptr<NodeTable>> nodeTables;
    NodesMetadata nodesMetadata;
    // Used to dynamically create nodeTables during checkpointing.
    bool isInMemoryMode;
};

} // namespace storage
} // namespace graphflow
