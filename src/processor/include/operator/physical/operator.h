#pragma once

#include <exception>
#include <memory>

#include "src/common/include/file_ser_deser_helper.h"
#include "src/processor/include/operator/physical/tuple/data_chunks.h"
#include "src/processor/include/task_system/morsel.h"
#include "src/storage/include/graph.h"

using namespace std;
using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class Operator {

public:
    Operator() = default;

    Operator(unique_ptr<Operator> prevOperator) {
        this->prevOperator.reset(prevOperator.release());
    }

    virtual ~Operator() = default;

    virtual bool hasNextMorsel() { return prevOperator->hasNextMorsel(); };

    // Warning: getNextTuples() should only be called if getNextMorsel() returns true.
    virtual void getNextTuples() = 0;

    shared_ptr<DataChunks> getDataChunks() { return dataChunks; };

    virtual unique_ptr<Operator> clone() = 0;

protected:
    shared_ptr<DataChunks> dataChunks;
    unique_ptr<Operator> prevOperator;
};

} // namespace processor
} // namespace graphflow
