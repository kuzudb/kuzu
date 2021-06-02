#pragma once

#include "src/processor/include/physical_plan/physical_plan.h"
#include "src/transaction/include/transaction.h"

using namespace graphflow::transaction;

namespace graphflow {
namespace processor {

struct ExecutionContext {

public:
    ExecutionContext(unique_ptr<PhysicalPlan> physicalPlan, uint64_t numThreads)
        : physicalPlan{move(physicalPlan)}, numThreads{numThreads} {};

public:
    unique_ptr<PhysicalPlan> physicalPlan;
    uint64_t numThreads = 1;
};

} // namespace processor
} // namespace graphflow
