#pragma once

#include <memory>

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/operator/operator.h"

namespace graphflow {
namespace processor {

class Scan : public Operator {

public:
    void getNextTuples() {}
    virtual void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);
    shared_ptr<NodeIDSequenceVector>& getNodeVector() { return nodeIDVector; }

protected:
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<NodeIDSequenceVector> nodeIDVector;
};

class ScanSingleLabel : public Scan {

public:
    bool getNextMorsel();
    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

protected:
    shared_ptr<MorselDescSingleLabelNodeIDs> morsel;
};

class ScanMultiLabel : public Scan {

public:
    bool getNextMorsel();
    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);

protected:
    shared_ptr<MorselDescMultiLabelNodeIDs> morsel;
};

} // namespace processor
} // namespace graphflow
