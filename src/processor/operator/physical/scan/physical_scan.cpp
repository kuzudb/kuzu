#include "src/processor/include/operator/physical/scan/physical_scan.h"

namespace graphflow {
namespace processor {

using lock_t = unique_lock<mutex>;

PhysicalScan::PhysicalScan(shared_ptr<MorselDesc>& morsel) : morsel{morsel} {
    dataChunks = make_shared<DataChunks>();
    nodeIDVector = make_shared<NodeIDSequenceVector>();
    outDataChunk = make_shared<DataChunk>();
    outDataChunk->append(nodeIDVector);
    dataChunks->append(outDataChunk);
}

bool PhysicalScan::hasNextMorsel() {
    lock_t lock{morsel->mtx};
    bool hasNextMorsel;
    if (morsel->currNodeOffset == morsel->maxNodeOffset) {
        // no more tuples to scan.
        hasNextMorsel = false;
    } else {
        nodeIDVector->setStartOffset(morsel->currNodeOffset);
        morsel->currNodeOffset += ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
        if (morsel->currNodeOffset >= morsel->maxNodeOffset) {
            morsel->currNodeOffset = morsel->maxNodeOffset;
        }
        currMorselNodeOffset = morsel->currNodeOffset;
        hasNextMorsel = true;
    }
    return hasNextMorsel;
}

void PhysicalScan::getNextTuples() {
    outDataChunk->size = currMorselNodeOffset < morsel->maxNodeOffset ?
                             ValueVector::NODE_SEQUENCE_VECTOR_SIZE :
                             morsel->maxNodeOffset % ValueVector::NODE_SEQUENCE_VECTOR_SIZE + 1;
}

} // namespace processor
} // namespace graphflow
