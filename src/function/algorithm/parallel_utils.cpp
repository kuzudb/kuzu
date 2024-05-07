#include "function/algorithm/parallel_utils.h"

namespace kuzu {
namespace graph {

bool ParallelUtils::init() {
    std::lock_guard<std::mutex> lck(mtx);
    if (!isMasterRegistered) {
        isMasterRegistered = true;
        return true;
    }
    return false;
}

bool ParallelUtils::runWorkerThread(function::TableFuncInput& input, function::TableFuncOutput& output) {
    std::unique_lock<std::mutex> lck{mtx, std::defer_lock};
    function::table_func_t tableFunction;
    while(true) {
        try {
            lck.lock();
            cv.wait(lck, [&] {
                if (!pendingQueue.empty()) {
                    tableFunction = pendingQueue.front();
                    totalThreadsRegistered++;
                    return true;
                }
                return !isActive;
            });
            lck.unlock();
            if (!isActive) {
                return false;
            }
            // the actual function to be executed, add it below this
            auto ret = tableFunction(input, output);
            lck.lock();
            if (--totalThreadsRegistered == 0 && ret == 0) {
                isComplete = true;
            }
            lck.unlock();
            cv.notify_all();
            if (ret > 0) {
                return true;
            }
        } catch (std::exception &e) {
            isActive.store(false, std::memory_order_relaxed);
            // also set an exception pointer
        }
    }
}

/*
 * 2 main steps involved:
 * (1) push the function pointer to the queue
 * (2) keep monitoring progress, isComplete flag
 * Once the worker thread indicates that task is complete or error is encountered, remove task
 * from queue.
 */
bool ParallelUtils::doParallel(function::TableFunction *tableFunction) {
    std::unique_lock<std::mutex> lck{mtx, std::defer_lock};
    lck.lock();
    pendingQueue.push_back(tableFunction->tableFunc);
    isComplete = false;
    lck.unlock();
    cv.notify_one();
    // now monitor progress
    while(true) {
        lck.lock();
        cv.wait(lck, [&] {
           if (isComplete) {
               return true;
           }
           return !isActive;
        });
        // main thread exited from loop, meaning either task is completed OR there was an error
        pendingQueue.pop_front();
        lck.unlock();
        // return true if task is completed (isComplete == true)
        return isComplete && isActive;
    }
}

void ParallelUtils::terminate() {
    isActive.store(false, std::memory_order_relaxed);
}

}
}