#pragma once

#include <memory>

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/operator/operator.h"

namespace graphflow {
namespace processor {

class Scan : public Operator {

public:
    Scan(string variableName) : variableName(variableName){};
    virtual void getNextTuples() = 0;
    virtual void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);
    void cleanup(){};
    shared_ptr<NodeIDSequenceVector>& getNodeVector() { return nodeIDVector; }

protected:
    string variableName;
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<NodeIDSequenceVector> nodeIDVector;
    node_offset_t currMorselNodeOffset;
};

class ScanSingleLabel : public Scan {

public:
    ScanSingleLabel(string variableName) : Scan(variableName){};
    void getNextTuples();
    bool hasNextMorsel();
    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

protected:
    shared_ptr<MorselDescSingleLabelNodeIDs> morsel;
};

class ScanMultiLabel : public Scan {

public:
    ScanMultiLabel(string variableName) : Scan(variableName){};
    void getNextTuples();
    bool hasNextMorsel();
    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

protected:
    shared_ptr<MorselDescMultiLabelNodeIDs> morsel;
    uint64_t currMorselLabelPos;
};

} // namespace processor
} // namespace graphflow
