#include "src/processor/include/physical_plan/operator/scan/physical_scan.h"

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
    if (morsel->currNodeOffset >= morsel->numNodes) {
        // no more tuples to scan.
        currentMorselStartOffset = -1u;
        currentMorselSize = -1u;
        return false;
    } else {
        currentMorselStartOffset = morsel->currNodeOffset;
        currentMorselSize = min((uint64_t)ValueVector::NODE_SEQUENCE_VECTOR_SIZE,
            morsel->numNodes - currentMorselStartOffset);
        morsel->currNodeOffset += currentMorselSize;
        return true;
    }
}

void PhysicalScan::getNextTuples() {
    nodeIDVector->setStartOffset(currentMorselStartOffset);
    outDataChunk->size = currentMorselSize;
}

} // namespace processor
} // namespace graphflow
