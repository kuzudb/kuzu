#pragma once

#include <exception>
#include <memory>

#include "src/processor/include/operator/tuple/data_chunk.h"
#include "src/processor/include/task_system/morsel.h"
#include "src/storage/include/graph.h"

using namespace std;
using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class Operator {
public:
    bool getNextMorsel() { return prevOperator->getNextMorsel(); }
    virtual void getNextTuples() = 0;
    virtual void initialize(Graph* graph, MorselDesc* morsel) = 0;

    vector<DataChunk*>& getDataChunks() { return dataChunks; }

    void setPrevOperator(Operator* prevOperator) {
        this->prevOperator = unique_ptr<Operator>(prevOperator);
    }

protected:
    vector<DataChunk*> dataChunks;
    unique_ptr<DataChunk> outDataChunk;
    unique_ptr<Operator> prevOperator;
};

} // namespace processor
} // namespace graphflow
