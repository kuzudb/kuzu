#include "function/algorithm/parallel_utils.h"

namespace kuzu {
namespace graph {

/*
 * returns true if the master has not registered, else false to indicate worker thread
 */
bool ParallelUtils::init() {
    std::lock_guard<std::mutex> lck(mtx);
    if (!isMasterRegistered) {
        isMasterRegistered = true;
        return true;
    }
    return false;
}

/*
 * each worker threads runs in a loop here, until it gets a function to execute
 * if the function returns > 0, exit from loop to return to parent operator
 * if the function returns 0 & total threads registered is 0 -> the function's execution is complete
 *
 * TableFuncInput and TableFuncOutput currently store the thread local state such as value vectors
 */
bool ParallelUtils::runWorkerThread(function::TableFuncInput& input,
    function::TableFuncOutput& output) {
    std::unique_lock<std::mutex> lck{mtx, std::defer_lock};
    std::shared_ptr<Task> task = nullptr;
    while (true) {
        try {
            lck.lock();
            cv.wait(lck, [&] {
                task = getTaskFromQueue();
                return task || !isActive;
            });
            lck.unlock();
            if (!isActive) {
                return false;
            }
            // the actual function to be executed, add it below this
            auto ret = task->function(input, output);
            task->deregisterThread();
            if (ret == 0) {
                task->completeTask(nullptr /* exception */);
                cv.notify_all();
            } else {
                return true;
            }
        } catch (std::exception& e) {
            task->completeTask(&e);
            cv.notify_all();
        }
    }
}

std::shared_ptr<Task> ParallelUtils::getTaskFromQueue() {
    if (pendingQueue.empty()) {
        return nullptr;
    }
    for (auto it : pendingQueue) {
        if (it->registerThread()) {
            return it;
        }
    }
    return nullptr;
}

/*
 * 2 main steps involved:
 * (1) push the function pointer to the queue
 * (2) keep monitoring progress, isComplete flag
 * Once the worker thread indicates that task is complete or error is encountered, remove task
 * from queue.
 */
bool ParallelUtils::doParallel(function::TableFunction* tableFunction) {
    std::unique_lock<std::mutex> lck{mtx, std::defer_lock};
    auto newTask = std::make_shared<Task>(tableFunction->tableFunc);
    lck.lock();
    pendingQueue.emplace_back(newTask);
    lck.unlock();
    cv.notify_one();
    // now monitor progress
    while (true) {
        lck.lock();
        cv.wait(lck, [&] {
            if (newTask->isCompleteOrHasException()) {
                return true;
            }
            return !isActive;
        });
        // main thread exited from loop, meaning either task is completed OR there was an error
        removeTaskFromQueueNoLock(newTask->ID);
        lck.unlock();
        // return true if task is completed (isComplete == true)
        return isActive;
    }
}

void ParallelUtils::removeTaskFromQueueNoLock(uint64_t taskID) {
    for (auto i = 0u; i < pendingQueue.size(); i++) {
        if (pendingQueue[i]->ID == taskID) {
            pendingQueue.erase(pendingQueue.begin() + i);
            return;
        }
    }
}

/*
 * Notify all worker threads to exit the infinite loop as the master has signalled no more functions
 * need to be executed.
 */
void ParallelUtils::terminate() {
    isActive.store(false, std::memory_order_relaxed);
    cv.notify_all();
}

} // namespace graph
} // namespace kuzu
