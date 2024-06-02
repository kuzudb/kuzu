#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "common/task_system/task_scheduler.h"
#include "function/table_functions.h"
#include "ife_morsel.h"
#include "processor/operator/algorithm/algorithm_runner_worker.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/operator/sink.h"

using namespace kuzu::processor;

namespace kuzu {
namespace graph {

class ParallelUtils {
public:
    explicit ParallelUtils(std::unique_ptr<AlgorithmRunnerWorker> algorithmRunnerWorker) :
    algorithmRunnerWorker{std::move(algorithmRunnerWorker)} {}

    inline function::TableFuncSharedState* getFuncSharedState() {
        return algorithmRunnerWorker->getInQuerySharedState();
    }

    inline void incrementTableFuncIdx() {
        algorithmRunnerWorker->incrementTableFuncIdx();
    }

    void doParallel(ExecutionContext* executionContext);

private:
    std::unique_ptr<AlgorithmRunnerWorker> algorithmRunnerWorker;
};

} // namespace graph
} // namespace kuzu
