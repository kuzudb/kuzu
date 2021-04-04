#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/tuple/data_chunks_iterator.h"

namespace graphflow {
namespace processor {

class Sink : public PhysicalOperator {

public:
    explicit Sink(unique_ptr<PhysicalOperator> prevOperator, PhysicalOperatorType operatorType)
        : PhysicalOperator{move(prevOperator), operatorType} {
        dataChunks = this->prevOperator->getDataChunks();
    };

    virtual void getNextTuples() override = 0;

    virtual unique_ptr<PhysicalOperator> clone() override = 0;

    virtual void finalize() {}
};

} // namespace processor
} // namespace graphflow
