#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class Sink : public PhysicalOperator {

public:
    Sink(unique_ptr<PhysicalOperator> prevOperator) : PhysicalOperator{move(prevOperator)} {};

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Sink>(prevOperator->clone());
    }

    uint64_t getNumTuples() { return numTuples; }

protected:
    uint64_t numTuples{0};
};

} // namespace processor
} // namespace graphflow
