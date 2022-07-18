#pragma once
#include <memory>
#include <vector>

#include "nodes_metadata.h"

#include "src/loader/in_mem_storage_structure/include/in_mem_column.h"
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

    // TODO: rename to getNodeTable
    inline NodeTable* getNode(label_t nodeLabel) const { return nodeTables[nodeLabel].get(); }

    void addNode(const Catalog& catalog, const string& labelName, BufferManager* bufferManager,
        bool isInMemory, const string& directory, WAL* wal);

private:
    static void createColumns(
        const string& directory, label_t nodeLabel, vector<Property> properties);

    static void createHashIndex(const string& directory, label_t nodeLabel, DataType pkDataType);

private:
    vector<unique_ptr<NodeTable>> nodeTables;
    NodesMetadata nodesMetadata;
};

} // namespace storage
} // namespace graphflow
