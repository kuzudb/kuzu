#pragma once

#include "src/common/include/types.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/structures/adj_lists.h"
#include "src/storage/include/structures/adj_rels.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace storage {

class RelsStore {

public:
    RelsStore(Catalog& catalog, vector<uint64_t>& numNodesPerLabel, const string& directory,
        BufferManager& bufferManager);

    inline static string getAdjRelsIndexFname(
        const string& directory, label_t relLabel, label_t nodeLabel, Direction direction) {
        return directory + "/r-" + to_string(relLabel) + "-" + to_string(nodeLabel) + "-" +
               to_string(direction) + ".col";
    }

    inline static string getAdjListsIndexFname(
        const string& directory, label_t relLabel, label_t nodeLabel, Direction direction) {
        return directory + "/r-" + to_string(relLabel) + "-" + to_string(nodeLabel) + "-" +
               to_string(direction) + ".lists";
    }

    inline static string getRelPropertyColumnFname(
        const string& directory, label_t relLabel, label_t nodeLabel, const string& propertyName) {
        return directory + "/r-" + to_string(relLabel) + "-" + to_string(nodeLabel) + "-" +
               propertyName + ".col";
    }

    inline static string getRelPropertyListsFname(const string& directory, label_t relLabel,
        label_t nodeLabel, Direction dir, const string& propertyName) {
        return directory + "/r-" + to_string(relLabel) + "-" + to_string(nodeLabel) + "-" +
               to_string(dir) + "-" + propertyName + ".lists";
    }
    static pair<uint32_t, uint32_t> getNumBytesScheme(const vector<label_t>& nbrNodeLabels,
        const vector<uint64_t>& numNodesPerLabel, uint32_t numNodeLabels);

private:
    static uint32_t getNumBytesForEncoding(uint64_t val, uint8_t minNumBytes);

    void initPropertyListsForRelLabel(Catalog& catalog, vector<uint64_t>& numNodesPerLabel,
        const string& directory, BufferManager& bufferManager);

    void initPropertyColumnsForRelLabel(Catalog& catalog, vector<uint64_t>& numNodesPerLabel,
        const string& directory, BufferManager& bufferManager, label_t relLabel, Direction dir);

    void initPropertyListsForRelLabel(Catalog& catalog, vector<uint64_t>& numNodesPerLabel,
        const string& directory, BufferManager& bufferManager, label_t relLabel);

private:
    vector<vector<vector<unique_ptr<BaseColumn>>>> propertyColumns;
    vector<vector<vector<vector<unique_ptr<Lists>>>>> propertyLists{2};
    vector<vector<vector<unique_ptr<AdjRels>>>> adjEdgesIndexes{2};
    vector<vector<vector<unique_ptr<AdjLists>>>> adjListsIndexes{2};
};

} // namespace storage
} // namespace graphflow
