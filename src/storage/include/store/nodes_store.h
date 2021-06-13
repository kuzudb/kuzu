#pragma once

#include <memory>
#include <vector>

#include "src/common/include/types.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/data_structure/column.h"
#include "src/storage/include/data_structure/lists/lists.h"
#include "src/storage/include/file_utils.h"

namespace graphflow {
namespace storage {

// NodesStore stores the properties of nodes in the system.
class NodesStore {

public:
    NodesStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
        const string& directory, BufferManager& bufferManager);

    BaseColumn* getNodePropertyColumn(const label_t& label, const uint64_t& propertyIdx) const {
        return propertyColumns[label][propertyIdx].get();
    }

    UnstructuredPropertyLists* getNodeUnstrPropertyLists(const label_t& label) const {
        return unstrPropertyLists[label].get();
    }

    inline static string getNodePropertyColumnFname(
        const string& directory, const label_t& nodeLabel, const string& propertyName) {
        return FileUtils::joinPath(
            directory, "n-" + to_string(nodeLabel) + "-" + propertyName + ".col");
    }

    inline static string getNodeUnstrPropertyListsFname(
        const string& directory, const label_t& nodeLabel) {
        return FileUtils::joinPath(directory, "n-" + to_string(nodeLabel) + ".lists");
    }

private:
    void initPropertyColumns(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
        const string& directory, BufferManager& bufferManager);

    void initUnstrPropertyLists(
        const Catalog& catalog, const string& directory, BufferManager& bufferManager);

private:
    shared_ptr<spdlog::logger> logger;
    // To store structured properties of nodes. There is one Property column for each unique
    // (node label, property) pair. That is, propertyColumns[4][5] refers to the property column
    // of node label 4 of property that have propertyIdx 5 in the propertyMap of label 4.
    vector<vector<unique_ptr<BaseColumn>>> propertyColumns;
    // To store unstructured properties of nodes.
    vector<unique_ptr<UnstructuredPropertyLists>> unstrPropertyLists;
};

} // namespace storage
} // namespace graphflow
