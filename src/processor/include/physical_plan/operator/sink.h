#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class Sink : public PhysicalOperator {

public:
    Sink(unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(child), context, id} {}

    PhysicalOperatorType getOperatorType() override = 0;

    shared_ptr<ResultSet> initResultSet() override = 0;

    virtual void execute() { initResultSet(); };

    bool getNextTuples() final { assert(false); }

    virtual void finalize(){};

    unique_ptr<PhysicalOperator> clone() override = 0;
};

} // namespace processor
} // namespace graphflow
