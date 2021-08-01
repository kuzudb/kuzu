#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class Skip : public PhysicalOperator {

public:
    Skip(uint64_t skipNumber, shared_ptr<atomic_uint64_t> counter,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Skip>(skipNumber, counter, prevOperator->clone(), context, id);
    }

private:
    uint64_t skipNumber;
    shared_ptr<atomic_uint64_t> counter;
};

} // namespace processor
} // namespace graphflow
