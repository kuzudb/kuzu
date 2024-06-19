#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "common/task_system/task_scheduler.h"
#include "function/table_functions.h"
#include "ife_morsel.h"
#include "processor/operator/parallel_utils_operator.h"

using namespace kuzu::processor;

namespace kuzu {
namespace function {

class ParallelUtils {
public:
    explicit ParallelUtils(uint32_t operatorID, common::TaskScheduler* taskScheduler);

    void doParallel(ExecutionContext* executionContext, GDSAlgorithm *gdsAlgorithm,
        GDSCallSharedState *gdsCallSharedState, gds_algofunc_t gdsAlgoFunc);

private:
    common::TaskScheduler *taskScheduler;
    uint32_t operatorID;
};

} // namespace graph
} // namespace kuzu
