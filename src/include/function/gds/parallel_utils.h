#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "common/task_system/task_scheduler.h"
#include "function/table_functions.h"
#include "ife_morsel.h"
#include "processor/operator/gds_call_worker.h"

using namespace kuzu::processor;

namespace kuzu {
namespace function {

class ParallelUtils {
public:
    explicit ParallelUtils(GDSCallInfo info,
        std::shared_ptr<GDSCallSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, uint32_t operatorID,
        common::TaskScheduler* taskScheduler, std::string expressions);

    inline std::shared_ptr<GDSCallSharedState>& getGDSCallSharedState() {
        return gdsCallWorker->getGDSCallSharedState();
    }

    void doParallel(ExecutionContext* executionContext, function::table_func_t tableFunc);

private:
    std::unique_ptr<GDSCallWorker> gdsCallWorker;
    common::TaskScheduler *taskScheduler;
};

} // namespace graph
} // namespace kuzu
