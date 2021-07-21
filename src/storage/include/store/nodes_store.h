#pragma once

#include <memory>
#include <vector>

#include "src/common/include/types.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/data_structure/column.h"
#include "src/storage/include/data_structure/lists/lists.h"
#include "src/storage/include/data_structure/lists/utils.h"

namespace graphflow {
namespace storage {

// NodesStore stores the properties of nodes in the system.
class NodesStore {

public:
    NodesStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
        const string& directory, BufferManager& bufferManager, bool isInMemoryMode);

    BaseColumn* getNodePropertyColumn(const label_t& label, const uint64_t& propertyIdx) const {
        return propertyColumns[label][propertyIdx].get();
    }

    UnstructuredPropertyLists* getNodeUnstrPropertyLists(const label_t& label) const {
        return unstrPropertyLists[label].get();
    }

    inline static string getNodePropertyColumnFName(
        const string& directory, const label_t& nodeLabel, const string& propertyName) {
        auto fName = StringUtils::string_format("n-%d-%s", nodeLabel, propertyName.data());
        return FileUtils::joinPath(directory, fName + BaseColumn::COLUMN_SUFFIX);
    }

    inline static string getNodeUnstrPropertyListsFName(
        const string& directory, const label_t& nodeLabel) {
        auto fName = StringUtils::string_format("n-%d", nodeLabel);
        return FileUtils::joinPath(directory, fName + BaseLists::LISTS_SUFFIX);
    }

private:
    void initStructuredPropertyColumns(const Catalog& catalog,
        const vector<uint64_t>& numNodesPerLabel, const string& directory,
        BufferManager& bufferManager, bool isInMemoryMode);

    void initUnstructuredPropertyLists(const Catalog& catalog, const string& directory,
        BufferManager& bufferManager, bool isInMemoryMode);

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
