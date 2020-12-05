#pragma once

#include <memory>

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/operator/operator.h"

namespace graphflow {
namespace processor {

class ScanSingleLabel : public Operator {
public:
    bool getNextMorsel();
    void getNextTuples() {}
    void initialize(Graph* graph, MorselDesc* morsel);
    NodeSequenceVector* getNodeVector() { return nodeVector.get(); }

protected:
    unique_ptr<NodeSequenceVector> nodeVector;
    MorselSequenceDesc* morsel;
};

class ScanMultiLabel : public Operator {
public:
    bool getNextMorsel();
    void getNextTuples() {}
    void initialize(Graph* graph, MorselDesc* morsel);
    NodeSequenceVector* getNodeVector() { return nodeVector.get(); }

protected:
    unique_ptr<NodeSequenceVector> nodeVector;
    MorselMultiLabelSequenceDesc* morsel;
};

} // namespace processor
} // namespace graphflow
