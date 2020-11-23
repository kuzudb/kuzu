#pragma once

#include <vector>

#include "src/storage/include/adjedges_index.h"
#include "src/storage/include/adjlists_index.h"
#include "src/storage/include/catalog.h"

namespace graphflow {
namespace storage {

class Indexes {

public:
    Indexes(Catalog& catalog, vector<uint64_t>& numNodesPerLabel, const string& directory,
        BufferManager& bufferManager);

    static pair<uint32_t, uint32_t> getNumBytesScheme(const vector<gfLabel_t>& nbrNodeLabels,
        const vector<uint64_t>& numNodesPerLabel, uint32_t numNodeLabels);

private:
    static uint32_t getNumBytesForEncoding(uint64_t val, uint8_t minNumBytes);

private:
    // adjEdgesIndexes are organized in a 3-dimensional matrix, wherein 0th dimension denotes the
    // direction {FORWARD=0, BACKWARD=1}, 1st dimension denotes the nodeLabel {source node label if
    // direction is FORWARD, otherwise destination node label} and the 3rd dimension denotes the
    // relationship label. For example, adjEdgesIndexes[0][4][5] is the adjEdges index for rel label
    // 5 in the FORWARD direction from nodes having node label 4.
    vector<vector<vector<unique_ptr<AdjEdgesBase>>>> adjEdgesIndexes{2};
    vector<vector<vector<unique_ptr<AdjLists>>>> adjListsIndexes{2};
};

} // namespace storage
} // namespace graphflow
