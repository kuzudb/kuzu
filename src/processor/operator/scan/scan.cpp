#include "src/processor/include/operator/scan/scan.h"

namespace graphflow {
namespace processor {

using lock_t = unique_lock<mutex>;

void ScanSingleLabel::initialize(Graph* graph, MorselDesc* morsel) {
    this->morsel = (MorselSequenceDesc*)morsel;
    outDataChunk = make_unique<DataChunk>();
    nodeVector = make_unique<NodeSequenceVector>();
    nodeVector->setLabel(this->morsel->nodeLabel);
    outDataChunk->append(*nodeVector);
    outDataChunk->size = VECTOR_SIZE;
    dataChunks.push_back(outDataChunk.get());
}

bool ScanSingleLabel::getNextMorsel() {
    lock_t lock{morsel->mtx};
    if (morsel->currNodeOffset == morsel->maxNodeOffset) {
        // no more tuples to scan.
        outDataChunk->size = 0;
        return false /* no new morsel */;
    }
    nodeVector->set(morsel->currNodeOffset);
    morsel->currNodeOffset += VECTOR_SIZE;
    if (morsel->currNodeOffset >= morsel->maxNodeOffset) {
        morsel->currNodeOffset = morsel->maxNodeOffset;
        outDataChunk->size = morsel->maxNodeOffset % VECTOR_SIZE + 1;
    }
    return true /* a new morsel obtained */;
}

void ScanMultiLabel::initialize(Graph* graph, MorselDesc* morsel) {
    this->morsel = (MorselMultiLabelSequenceDesc*)morsel;
    outDataChunk = make_unique<DataChunk>();
    nodeVector = make_unique<NodeSequenceVector>();
    outDataChunk->append(*nodeVector);
    outDataChunk->size = VECTOR_SIZE;
    dataChunks.push_back(outDataChunk.get());
}

bool ScanMultiLabel::getNextMorsel() {
    lock_t lock{morsel->mtx};
    uint64_t index = morsel->currPos;
    if (morsel->currNodeOffset == morsel->maxNodeOffset[index]) {
        if (index == morsel->numLabels) {
            // no more tuples to scan.
            outDataChunk->size = 0;
            return false /* no new morsel */;
        }
        // all nodes of current label are scanned, move to the next label.
        index += 1;
        morsel->currPos += 1;
        morsel->currNodeOffset = 0;
        outDataChunk->size = VECTOR_SIZE;
    }

    nodeVector->setLabel(morsel->nodeLabel[index]);
    nodeVector->set(morsel->currNodeOffset);
    morsel->currNodeOffset += VECTOR_SIZE;
    auto maxOffset = morsel->maxNodeOffset[index];
    if (morsel->currNodeOffset > maxOffset) {
        morsel->currNodeOffset = maxOffset;
        outDataChunk->size = maxOffset % VECTOR_SIZE + 1;
    }
    return true /* a new morsel obtained */;
}

} // namespace processor
} // namespace graphflow
