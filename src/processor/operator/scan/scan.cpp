#include "src/processor/include/operator/scan/scan.h"

#include <memory>

#include "src/processor/include/operator/operator_ser_deser_factory.h"

namespace graphflow {
namespace processor {

using lock_t = unique_lock<mutex>;

Scan::Scan(FileDeserHelper& fdsh) : variableName{*fdsh.readString()} {};

void Scan::initialize(Graph* graph) {
    dataChunks = make_shared<DataChunks>();
    nodeIDVector = make_shared<NodeIDSequenceVector>(variableName);
    outDataChunk = make_shared<DataChunk>();
    outDataChunk->append(nodeIDVector);
    dataChunks->append(outDataChunk);
}

void Scan::serialize(FileSerHelper& fsh) {
    fsh.writeString(variableName);
}

ScanSingleLabel::ScanSingleLabel(FileDeserHelper& fdsh)
    : Scan(fdsh), morsel{make_shared<MorselDescSingleLabelNodeIDs>(fdsh)} {
    cout << "here" << endl;
}

void ScanSingleLabel::initialize(Graph* graph) {
    Scan::initialize(graph);
    nodeIDVector->setLabel(this->morsel->nodeLabel);
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

void ScanSingleLabel::serialize(FileSerHelper& fsh) {
    string typeIDStr = typeid(ScanSingleLabel).name();
    fsh.writeString(typeIDStr);
    Scan::serialize(fsh);
    morsel->serialize(fsh);
}

ScanMultiLabel::ScanMultiLabel(FileDeserHelper& fdsh)
    : Scan(fdsh), morsel{make_shared<MorselDescMultiLabelNodeIDs>(fdsh)} {}

bool ScanMultiLabel::hasNextMorsel() {
    lock_t lock{morsel->mtx};
    currMorselLabelPos = morsel->currPos;
    if (morsel->currNodeOffset == (*morsel->maxNodeOffsets)[currMorselLabelPos]) {
        if (currMorselLabelPos == morsel->numLabels) {
            return false;
        }
        // all nodes of current label are scanned, move to the next label.
        currMorselLabelPos += 1;
        morsel->currPos += 1;
        morsel->currNodeOffset = 0;
    }
    nodeIDVector->setLabel((*morsel->nodeLabels)[currMorselLabelPos]);
    nodeIDVector->setStartOffset(morsel->currNodeOffset);
    morsel->currNodeOffset += ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
    auto maxOffset = (*morsel->maxNodeOffsets)[currMorselLabelPos];
    if (morsel->currNodeOffset >= maxOffset) {
        morsel->currNodeOffset = maxOffset;
    }
    currMorselNodeOffset = morsel->currNodeOffset;
    return true;
}

void ScanMultiLabel::getNextTuples() {
    auto maxOffset = (*morsel->maxNodeOffsets)[currMorselLabelPos];
    outDataChunk->size = currMorselNodeOffset == maxOffset ?
                             maxOffset % ValueVector::NODE_SEQUENCE_VECTOR_SIZE + 1 :
                             ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
}

void ScanMultiLabel::serialize(FileSerHelper& fsh) {
    string typeIDStr = typeid(ScanMultiLabel).name();
    fsh.writeString(typeIDStr);
    Scan::serialize(fsh);
    morsel->serialize(fsh);
}

} // namespace processor
} // namespace graphflow
