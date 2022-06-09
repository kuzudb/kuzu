#pragma once

#include "src/catalog/include/catalog.h"
#include "src/storage/index/include/hash_index.h"
#include "src/storage/storage_structure/include/column.h"
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
    NodeTable(NodeMetadata* nodeMetadata, BufferManager& bufferManager, bool isInMemory,
        const vector<catalog::Property>& properties, const string& directory, WAL* wal);

    inline Column* getPropertyColumn(uint64_t propertyIdx) {
        return propertyColumns[propertyIdx].get();
    }
    inline UnstructuredPropertyLists* getUnstrPropertyLists() const {
        return unstrPropertyLists.get();
    }
    inline HashIndex* getIDIndex() const { return IDIndex.get(); }

    // TODO(Xiyang/Guodong): We might want to change pos -> sel_t selectedPos depending on how the
    // Delete operator is implemented. Note that this function is not tested.
    void deleteNode(ValueVector nodeOffsetVector, ValueVector primaryKeyVector, uint64_t pos) {
        node_offset_t nodeOffset = nodeOffsetVector.readNodeOffset(pos);
        nodeMetadata->deleteNode(nodeOffset);
        switch (primaryKeyVector.dataType.typeID) {
        case INT64: {
            IDIndex->deleteKey(((uint64_t*)primaryKeyVector.values)[pos]);
        } break;
        case STRING: {
            IDIndex->deleteKey(((gf_string_t*)primaryKeyVector.values)[pos].getAsString().c_str());
        } break;
        default:
            throw RuntimeException(
                "Trying to delete a node whose primary key ValueVector is not INT64 or STRING");
        }
    }

public:
    NodeMetadata* nodeMetadata;

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
