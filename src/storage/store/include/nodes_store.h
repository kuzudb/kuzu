#pragma once

#include <memory>
#include <vector>

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

private:
    vector<unique_ptr<NodeTable>> nodeTables;
};

} // namespace storage
} // namespace graphflow
