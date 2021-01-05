#pragma once

#include <exception>
#include <memory>

#include "src/common/include/file_ser_deser_helper.h"
#include "src/processor/include/operator/tuple/data_chunks.h"
#include "src/processor/include/task_system/morsel.h"
#include "src/storage/include/graph.h"

using namespace std;
using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class Operator {

public:
    Operator() = default;

    virtual void initialize(Graph* graph) = 0;

    virtual bool hasNextMorsel() { return prevOperator->hasNextMorsel(); };

    // Warning: getNextTuples() should only be called if getNextMorsel() returns true.
    virtual void getNextTuples() = 0;

    shared_ptr<DataChunks> getOutDataChunks() { return dataChunks; };

    virtual void cleanup() { prevOperator->cleanup(); };

    virtual unique_ptr<Operator> clone() = 0;

    virtual void serialize(FileSerHelper& fsh) { prevOperator->serialize(fsh); };

    void setPrevOperator(unique_ptr<Operator> prevOperator) {
        this->prevOperator.reset(prevOperator.release());
    };

protected:
    shared_ptr<DataChunks> dataChunks;
    unique_ptr<Operator> prevOperator;
};

} // namespace processor
} // namespace graphflow
