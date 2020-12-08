#pragma once

#include "src/processor/include/operator/operator.h"

namespace graphflow {
namespace processor {

class Sink : public Operator {
public:
    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel);
    void getNextTuples();

protected:
    uint64_t numTuples;
};

} // namespace processor
} // namespace graphflow
