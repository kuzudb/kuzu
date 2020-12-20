#pragma once

#include "src/common/include/types.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/structures/adj_lists.h"
#include "src/storage/include/structures/column.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace storage {

class RelsStore {

public:
    RelsStore(const Catalog& catalog, const vector<uint64_t>& numNodesPerLabel,
        const string& directory, BufferManager& bufferManager);

    Column* getRelPropertyColumn(
        const label_t& relLabel, const label_t& nodeLabel, const uint64_t& propertyIdx) {
        return propertyColumns[relLabel][nodeLabel][propertyIdx].get();
    }

    Column* getAdjColumn(
        const Direction& direction, const label_t& nodeLabel, const label_t& relLabel) {
        return adjColumnIndexes[direction][nodeLabel][relLabel].get();
    }

    inline static string getAdjColumnIndexFname(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const Direction& direction) {
        return directory + "/r-" + to_string(relLabel) + "-" + to_string(nodeLabel) + "-" +
               to_string(direction) + ".col";
    }

    inline static string getAdjListsIndexFname(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const Direction& direction) {
        return directory + "/r-" + to_string(relLabel) + "-" + to_string(nodeLabel) + "-" +
               to_string(direction) + ".lists";
    }

    inline static string getRelPropertyColumnFname(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const string& propertyName) {
        return directory + "/r-" + to_string(relLabel) + "-" + to_string(nodeLabel) + "-" +
               propertyName + ".col";
    }

    inline static string getRelPropertyListsFname(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const Direction& dir, const string& propertyName) {
        return directory + "/r-" + to_string(relLabel) + "-" + to_string(nodeLabel) + "-" +
               to_string(dir) + "-" + propertyName + ".lists";
    }

    static pair<uint32_t, uint32_t> getNumBytesScheme(const vector<label_t>& nbrNodeLabels,
        const vector<uint64_t>& numNodesPerLabel, const uint32_t& numNodeLabels);

private:
    static uint32_t getNumBytesForEncoding(const uint64_t& val, const uint8_t& minNumBytes);

    void initPropertyListsForRelLabel(const Catalog& catalog,
        const vector<uint64_t>& numNodesPerLabel, const string& directory,
        BufferManager& bufferManager);

    void initPropertyColumnsForRelLabel(const Catalog& catalog,
        const vector<uint64_t>& numNodesPerLabel, const string& directory,
        BufferManager& bufferManager, const label_t& relLabel, const Direction& dir);

    void initPropertyListsForRelLabel(const Catalog& catalog,
        const vector<uint64_t>& numNodesPerLabel, const string& directory,
        BufferManager& bufferManager, const label_t& relLabel);

private:
    vector<vector<vector<unique_ptr<Column>>>> propertyColumns;
    vector<vector<vector<vector<unique_ptr<Lists>>>>> propertyLists{2};
    vector<vector<vector<unique_ptr<Column>>>> adjColumnIndexes{2};
    vector<vector<vector<unique_ptr<AdjLists>>>> adjListsIndexes{2};
};

} // namespace storage
} // namespace graphflow
