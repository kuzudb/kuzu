#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "function/table_functions.h"

namespace kuzu {
namespace graph {

static std::atomic<uint64_t> taskIDCounter = 0LU;

class Task {
public:
    explicit Task(function::table_func_t function)
        : function{function}, error{nullptr}, isComplete{false}, ID{taskIDCounter.fetch_add(1)},
          totalThreadsRegistered{0u} {}

    bool registerThread() {
        std::unique_lock lck{mtx};
        if (!isComplete) {
            totalThreadsRegistered++;
            return true;
        }
        return false;
    }

    void deregisterThread() {
        std::unique_lock lck{mtx};
        totalThreadsRegistered--;
    }

    void completeTask(std::exception* exp) {
        std::unique_lock lck{mtx};
        isComplete = true;
        error = exp;
    }

    bool isCompleteOrHasException() {
        std::unique_lock lck{mtx};
        return isComplete || error;
    }

public:
    function::table_func_t function;
    std::exception* error;
    bool isComplete;
    uint64_t ID;

private:
    std::mutex mtx;
    uint64_t totalThreadsRegistered;
};

class ParallelUtils {
public:
    ParallelUtils() : isActive{true}, isMasterRegistered{false} {}

    bool init();

    bool runWorkerThread(function::TableFuncInput& input, function::TableFuncOutput& output);

    std::shared_ptr<Task> getTaskFromQueue();

    bool doParallel(function::TableFunction* tableFunction);

    void removeTaskFromQueueNoLock(uint64_t taskID);

    void terminate();

private:
    std::mutex mtx;
    std::atomic_bool isActive;
    bool isMasterRegistered;
    std::condition_variable cv;
    std::deque<std::shared_ptr<Task>> pendingQueue;
};

} // namespace graph
} // namespace kuzu
