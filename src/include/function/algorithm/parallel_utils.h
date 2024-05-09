#pragma once

#include <mutex>
#include <atomic>
#include <condition_variable>
#include "function/table_functions.h"

namespace kuzu {
namespace graph {

class ParallelUtils {
public:
    ParallelUtils() : isActive{true}, isComplete(false), isMasterRegistered{false}, totalThreadsRegistered{0u} {}

    bool init();

    bool runWorkerThread(function::TableFuncInput& input, function::TableFuncOutput& output);

    bool doParallel(function::TableFunction *tableFunction);

    void terminate();

private:
    std::mutex mtx;
    std::atomic_bool isActive;
    bool isComplete;
    bool isMasterRegistered;
    std::condition_variable cv;
    uint64_t totalThreadsRegistered;
    std::deque<function::table_func_t> pendingQueue;
};

}
}
