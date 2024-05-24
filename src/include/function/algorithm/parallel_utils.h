#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "common/task_system/task_scheduler.h"
#include "function/table_functions.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/operator/sink.h"

using namespace kuzu::processor;

namespace kuzu {
namespace graph {

class ParallelUtils {
public:
    explicit ParallelUtils(std::shared_ptr<FactorizedTable> globalFTable) :
          globalFTable{std::move(globalFTable)}, registerMaster{false} {}

    bool init();

    void mergeResults(FactorizedTable *factorizedTable);

    void doParallel(Sink *sink, ExecutionContext* executionContext);

    void doParallel(Sink *sink, ExecutionContext *executionContext, function::table_func_t func);

private:
    std::mutex mtx;
    std::shared_ptr<FactorizedTable> globalFTable;
    bool registerMaster;
};

} // namespace graph
} // namespace kuzu
