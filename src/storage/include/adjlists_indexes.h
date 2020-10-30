#ifndef GRAPHFLOW_STORAGE_ADJLISTS_INDEXES_H_
#define GRAPHFLOW_STORAGE_ADJLISTS_INDEXES_H_

#include <vector>

#include "src/storage/include/catalog.h"
#include "src/storage/include/column.h"

namespace graphflow {
namespace storage {

class AdjListsIndexes {

public:
    AdjListsIndexes(Catalog &catalog, vector<uint64_t> &numNodesPerLabel, const string &directory,
        BufferManager &bufferManager);

    static pair<uint32_t, uint32_t> getColumnAdjListsIndexScheme(
        const vector<gfLabel_t> &nbrNodeLabels, const vector<uint64_t> &numNodesPerLabel,
        uint32_t numNodeLabels);

    inline static string getColumnAdjListIndexFname(
        const string &directory, gfLabel_t relLabel, gfLabel_t nodeLabel, Direction direction) {
        return directory + "/e-" + to_string(relLabel) + "-" + to_string(nodeLabel) + "-" +
               to_string(direction) + ".vcol";
    }

private:
    void initSingleCardinalityAdjListsIndexes(Catalog &catalog, vector<uint64_t> &numNodesPerLabel,
        const string &directory, BufferManager &bufferManager);

    unique_ptr<ColumnBase> initSingleCardinalityAdjListsIndex(
        const vector<gfLabel_t> &nbrNodeLabels, const vector<uint64_t> &numNodesPerLabel,
        uint32_t numNodeLabels, const string &directory, gfLabel_t nodeLabel, gfLabel_t relLabel,
        Direction direction, BufferManager &bufferManager);
    
    static uint32_t getNumBytesForEncoding(uint64_t val);

private:
    // Single Cardinality Adjlists are organized in a 3-dimensional matrix, wherein 0th dimension
    // denotes the direction {FORWARD=0, BACKWARD=1}, 1st dimension denotes the nodeLabel {source
    // node label if direction is FORWARD, otherwise destination node label} and the 3rd dimension
    // denotes the relationship label. For Example, singleCardinalityAdjLists[0][4][5] is the
    // columnar adjacency lists for rel label 5 in the FORWARD direction from nodes having node
    // label 4.
    vector<vector<vector<unique_ptr<ColumnBase>>>> singleCardinalityAdjLists;
};

} // namespace storage
} // namespace graphflow

#endif // GRAPHFLOW_STORAGE_ADJLISTS_INDEXES_H_
