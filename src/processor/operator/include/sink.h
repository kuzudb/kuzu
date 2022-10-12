#pragma once

#include "src/processor/operator/include/physical_operator.h"

namespace graphflow {
namespace processor {

class Sink : public PhysicalOperator {

public:
    Sink(unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString} {}

    PhysicalOperatorType getOperatorType() override = 0;

    virtual void execute(ExecutionContext* context) = 0;

    bool getNextTuples() final { assert(false); }

    virtual void finalize(ExecutionContext* context){};

    unique_ptr<PhysicalOperator> clone() override = 0;
};

} // namespace processor
} // namespace graphflow
