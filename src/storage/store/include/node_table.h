#pragma once

#include "src/catalog/include/catalog.h"
#include "src/storage/index/include/hash_index.h"
#include "src/storage/storage_structure/include/lists/lists.h"
#include "src/storage/storage_structure/include/lists/unstructured_property_lists.h"
#include "src/storage/store/include/nodes_metadata.h"
#include "src/storage/wal/include/wal.h"

namespace spdlog {
class logger;
}

namespace graphflow {
namespace storage {

class NodeTable {

public:
    NodeTable(NodesMetadata* nodesMetadata, BufferManager& bufferManager, bool isInMemory, WAL* wal,
        NodeLabel* nodeLabel);

    inline Column* getPropertyColumn(uint64_t propertyIdx) {
        return propertyColumns[propertyIdx].get();
    }
    inline UnstructuredPropertyLists* getUnstrPropertyLists() const {
        return unstrPropertyLists.get();
    }
    inline HashIndex* getIDIndex() const { return IDIndex.get(); }

    inline NodesMetadata* getNodesMetadata() const { return nodesMetadata; }

    inline label_t getLabelID() const { return labelID; }

    void deleteNodes(ValueVector* nodeIDVector, ValueVector* primaryKeyVector);

private:
    void deleteNode(ValueVector* nodeIDVector, ValueVector* primaryKeyVector, uint32_t pos) const;

public:
    NodesMetadata* nodesMetadata;
    label_t labelID;

private:
    // This is for structured properties.
    vector<unique_ptr<Column>> propertyColumns;
    // All unstructured properties of a node are stored inside one UnstructuredPropertyLists.
    unique_ptr<UnstructuredPropertyLists> unstrPropertyLists;
    // The index for ID property.
    unique_ptr<HashIndex> IDIndex;
};

} // namespace storage
} // namespace graphflow
