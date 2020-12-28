#include "src/processor/include/operator/scan/scan.h"

namespace graphflow {
namespace processor {

using lock_t = unique_lock<mutex>;

void Scan::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    nodeIDVector = make_shared<NodeIDSequenceVector>(variableName);
    outDataChunk = make_shared<DataChunk>();
    outDataChunk->append(nodeIDVector);
    dataChunks->append(outDataChunk);
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

bool ScanSingleLabel::hasNextMorsel() {
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

void ScanSingleLabel::getNextTuples() {
    outDataChunk->size = currMorselNodeOffset < morsel->maxNodeOffset ?
                             ValueVector::NODE_SEQUENCE_VECTOR_SIZE :
                             morsel->maxNodeOffset % ValueVector::NODE_SEQUENCE_VECTOR_SIZE + 1;
}

bool ScanMultiLabel::hasNextMorsel() {
    lock_t lock{morsel->mtx};
    currMorselLabelPos = morsel->currPos;
    if (morsel->currNodeOffset == morsel->maxNodeOffset[currMorselLabelPos]) {
        if (currMorselLabelPos == morsel->numLabels) {
            // no more tuples to scan.
            return false /* has no next morsel */;
        }
        // all nodes of current label are scanned, move to the next label.
        currMorselLabelPos += 1;
        morsel->currPos += 1;
        morsel->currNodeOffset = 0;
    }

    nodeIDVector->setLabel(morsel->nodeLabel[currMorselLabelPos]);
    nodeIDVector->setStartOffset(morsel->currNodeOffset);
    morsel->currNodeOffset += ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
    auto maxOffset = morsel->maxNodeOffset[currMorselLabelPos];
    if (morsel->currNodeOffset >= maxOffset) {
        morsel->currNodeOffset = maxOffset;
    }
    currMorselNodeOffset = morsel->currNodeOffset;
    return true /* has a next morsel */;
}

void ScanMultiLabel::getNextTuples() {
    auto maxOffset = morsel->maxNodeOffset[currMorselLabelPos];
    outDataChunk->size = currMorselNodeOffset == maxOffset ?
                             maxOffset % ValueVector::NODE_SEQUENCE_VECTOR_SIZE + 1 :
                             ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
}

} // namespace processor
} // namespace graphflow
