#pragma once

#include "src/catalog/include/catalog.h"
#include "src/storage/include/index/hash_index.h"
#include "src/storage/include/storage_structure/column.h"
#include "src/storage/include/storage_structure/lists/lists.h"
#include "src/storage/include/storage_structure/lists/unstructured_property_lists.h"
#include "src/storage/include/wal/wal.h"

namespace graphflow {
namespace storage {

class Node {

public:
    Node(label_t labelID, BufferManager& bufferManager, bool isInMemory,
        const vector<catalog::Property>& properties, const string& directory, WAL* wal);

    inline Column* getPropertyColumn(uint64_t propertyIdx) {
        return propertyColumns[propertyIdx].get();
    }
    inline UnstructuredPropertyLists* getUnstrPropertyLists() const {
        return unstrPropertyLists.get();
    }
    inline HashIndex* getIDIndex() const { return IDIndex.get(); }

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
