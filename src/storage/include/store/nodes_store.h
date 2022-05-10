#pragma once

#include <memory>
#include <vector>

#include "src/storage/include/store/node.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace storage {

class NodesStore {

public:
    NodesStore(const Catalog& catalog, BufferManager& bufferManager, const string& directory,
        bool isInMemoryMode, WAL* wal);

    inline Column* getNodePropertyColumn(label_t nodeLabel, uint64_t propertyIdx) const {
        return nodes[nodeLabel]->getPropertyColumn(propertyIdx);
    }
    inline UnstructuredPropertyLists* getNodeUnstrPropertyLists(label_t nodeLabel) const {
        return nodes[nodeLabel]->getUnstrPropertyLists();
    }

private:
    shared_ptr<spdlog::logger> logger;
    vector<unique_ptr<Node>> nodes;
};

} // namespace storage
} // namespace graphflow
