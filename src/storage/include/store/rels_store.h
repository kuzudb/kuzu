#pragma once

#include "src/common/include/file_utils.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/data_structure/column.h"
#include "src/storage/include/data_structure/lists/lists.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace storage {

// RelsStore stores adjacent rels of a node as well as the properties of rels in the system.
class RelsStore {

public:
    RelsStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
        const string& directory, BufferManager& bufferManager);

    inline BaseColumn* getRelPropertyColumn(
        const label_t& relLabel, const label_t& nodeLabel, const uint64_t& propertyIdx) const {
        return propertyColumns[nodeLabel][relLabel][propertyIdx].get();
    }

    inline BaseLists* getRelPropertyLists(const Direction& direction, const label_t& nodeLabel,
        const label_t& relLabel, const uint64_t& propertyIdx) const {
        return propertyLists[direction][nodeLabel][relLabel][propertyIdx].get();
    }

    inline AdjColumn* getAdjColumn(
        const Direction& direction, const label_t& nodeLabel, const label_t& relLabel) const {
        return adjColumns[direction][nodeLabel][relLabel].get();
    }

    inline AdjLists* getAdjLists(
        const Direction& direction, const label_t& nodeLabel, const label_t& relLabel) const {
        return adjLists[direction][nodeLabel][relLabel].get();
    }

    inline static string getAdjColumnFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const Direction& direction) {
        return FileUtils::joinPath(directory, "r-" + to_string(relLabel) + "-" +
                                                  to_string(nodeLabel) + "-" +
                                                  to_string(direction) + ".col");
    }

    inline static string getAdjListsFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const Direction& direction) {
        return FileUtils::joinPath(directory, "r-" + to_string(relLabel) + "-" +
                                                  to_string(nodeLabel) + "-" +
                                                  to_string(direction) + ".lists");
    }

    inline static string getRelPropertyColumnFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const string& propertyName) {
        return FileUtils::joinPath(directory,
            "r-" + to_string(relLabel) + "-" + to_string(nodeLabel) + "-" + propertyName + ".col");
    }

    inline static string getRelPropertyListsFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const Direction& direction, const string& propertyName) {
        return FileUtils::joinPath(
            directory, "r-" + to_string(relLabel) + "-" + to_string(nodeLabel) + "-" +
                           to_string(direction) + "-" + propertyName + ".lists");
    }

private:
    void initAdjColumns(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
        const string& directory, BufferManager& bufferManager);

    void initAdjLists(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
        const string& directory, BufferManager& bufferManager);

    void initPropertyListsAndColumns(const Catalog& catalog,
        const vector<uint64_t>& numNodesPerLabel, const string& directory,
        BufferManager& bufferManager);

    void initPropertyColumnsForRelLabel(const Catalog& catalog,
        const vector<uint64_t>& numNodesPerLabel, const string& directory,
        BufferManager& bufferManager, const label_t& relLabel, const Direction& direction);

    void initPropertyListsForRelLabel(const Catalog& catalog,
        const vector<uint64_t>& numNodesPerLabel, const string& directory,
        BufferManager& bufferManager, const label_t& relLabel);

private:
    shared_ptr<spdlog::logger> logger;
    // propertyColumns are organized in 2-dimensional vectors wherein the first dimension gives
    // nodeLabel and the second dimension is the relLabel.
    vector<vector<vector<unique_ptr<BaseColumn>>>> propertyColumns;
    // propertyLists, adjColumns and adjLists are organized in 3-dimensional vectors wherein the
    // first dimension gives direction, second dimension gives nodeLabel and the third dimension is
    // the relLabel.
    vector<vector<vector<vector<unique_ptr<BaseLists>>>>> propertyLists{2};
    vector<vector<vector<unique_ptr<AdjColumn>>>> adjColumns{2};
    vector<vector<vector<unique_ptr<AdjLists>>>> adjLists{2};
};

} // namespace storage
} // namespace graphflow
