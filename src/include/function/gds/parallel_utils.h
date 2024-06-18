#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "common/task_system/task_scheduler.h"
#include "function/table_functions.h"
// #include "ife_morsel.h"
// #include "processor/operator/gds_call_worker.h"

using namespace kuzu::processor;

namespace kuzu {
namespace function {

class ParallelUtils {
public:
    explicit ParallelUtils(common::TaskScheduler* taskScheduler, uint32_t operatorID);

//    inline std::shared_ptr<GDSCallSharedState>& getGDSCallSharedState() {
//        return gdsCallWorker->getGDSCallSharedState();
//    }

    void countParallel(ExecutionContext* executionContext, int64_t count);

private:
//    std::unique_ptr<GDSCallWorker> gdsCallWorker;
    common::TaskScheduler *taskScheduler;
    uint32_t operatorID;
};

} // namespace graph
} // namespace kuzu