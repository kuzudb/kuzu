#include "src/storage/include/compression_scheme.h"

#include <stdexcept>

namespace graphflow {
namespace common {

NodeIDCompressionScheme::NodeIDCompressionScheme(const unordered_set<table_id_t>& nbrNodeTableIDs,
    const vector<node_offset_t>& maxNodeOffsetsPerTable)
    : commonTableID(0) {
    uint64_t maxNumNodeOffsetToFit = 0;
    for (auto nbrNodeTableID : nbrNodeTableIDs) {
        if (maxNodeOffsetsPerTable[nbrNodeTableID] + 1 > maxNumNodeOffsetToFit) {
            maxNumNodeOffsetToFit = maxNodeOffsetsPerTable[nbrNodeTableID] + 1;
        }
    }
    if (nbrNodeTableIDs.size() == 1) {
        numBytesForTableID = 0;
        commonTableID = *nbrNodeTableIDs.begin();
    } else {
        numBytesForTableID =
            getNumBytesForEncoding(maxNodeOffsetsPerTable.size() - 1, 1 /* min num bytes */);
    }
    numBytesForOffset = getNumBytesForEncoding(maxNumNodeOffsetToFit, 2 /*min num bytes*/);
}

uint32_t NodeIDCompressionScheme::getNumBytesForEncoding(
    uint64_t maxValToEncode, uint8_t minNumBytes) {
    auto numBytes = minNumBytes;
    while (maxValToEncode > (1ull << (8 * numBytes)) - 2) {
        numBytes <<= 1;
    }
    return numBytes;
}

} // namespace common
} // namespace graphflow
