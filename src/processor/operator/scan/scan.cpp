#include "src/processor/include/operator/scan/scan.h"

namespace graphflow {
namespace processor {

using lock_t = unique_lock<mutex>;

void Scan::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    outDataChunk = make_shared<DataChunk>();
    nodeIDVector = make_shared<NodeIDSequenceVector>();
    outDataChunk->append(nodeIDVector);
    outDataChunk->size = ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
    outDataChunks.push_back(outDataChunk);
}

void ScanSingleLabel::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    Scan::initialize(graph, morsel);
    this->morsel = static_pointer_cast<MorselDescSingleLabelNodeIDs>(morsel);
    nodeIDVector->setLabel(this->morsel->nodeLabel);
}

void ScanMultiLabel::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    Scan::initialize(graph, morsel);
    this->morsel = static_pointer_cast<MorselDescMultiLabelNodeIDs>(morsel);
}

bool ScanSingleLabel::getNextMorsel() {
    lock_t lock{morsel->mtx};
    if (morsel->currNodeOffset == morsel->maxNodeOffset) {
        // no more tuples to scan.
        outDataChunk->size = 0;
        return false /* no new morsel */;
    }
    nodeIDVector->setStartOffset(morsel->currNodeOffset);
    morsel->currNodeOffset += ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
    if (morsel->currNodeOffset >= morsel->maxNodeOffset) {
        morsel->currNodeOffset = morsel->maxNodeOffset;
        outDataChunk->size = morsel->maxNodeOffset % ValueVector::NODE_SEQUENCE_VECTOR_SIZE + 1;
    }
    return true /* a new morsel obtained */;
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
        outDataChunk->size = ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
    }

    nodeIDVector->setLabel(morsel->nodeLabel[index]);
    nodeIDVector->setStartOffset(morsel->currNodeOffset);
    morsel->currNodeOffset += ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
    auto maxOffset = morsel->maxNodeOffset[index];
    if (morsel->currNodeOffset >= maxOffset) {
        morsel->currNodeOffset = maxOffset;
        outDataChunk->size = maxOffset % ValueVector::NODE_SEQUENCE_VECTOR_SIZE + 1;
    }
    return true /* a new morsel obtained */;
}

} // namespace processor
} // namespace graphflow
