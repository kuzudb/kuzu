#pragma once

#include <memory>
#include <vector>

#include "src/common/include/types.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/structures/column.h"

namespace graphflow {
namespace storage {

// NodeStore stores the properties of nodes in the system.
class NodesStore {

public:
    NodesStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
        const string& directory, BufferManager& bufferManager);

    Column* getNodePropertyColumn(const label_t& label, const uint64_t& propertyIdx) {
        return propertyColumns[label][propertyIdx].get();
    }

    inline static string getNodePropertyColumnFname(
        const string& directory, const label_t& nodeLabel, const string& propertyName) {
        return directory + "/n-" + to_string(nodeLabel) + "-" + propertyName + ".col";
    }

private:
    // The properties of nodes in the system are stored in Property Columns with one column for each
    // unique (node label, property) pair. Here, propertyColumns[4][5] refers to the property column
    // of node label 4 of property that have propertyIdx 5 in the propertyMap of label 4.
    vector<vector<unique_ptr<Column>>> propertyColumns;
};

} // namespace storage
} // namespace graphflow
