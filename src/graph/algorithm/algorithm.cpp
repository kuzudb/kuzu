#include "graph/algorithm.h"

namespace kuzu {
namespace graph {

void Algorithm::startExecute() {
    mtx.lock();
    if (masterIsSelected && totalThreadsRegistered < config.maxThreadsForExecution) {
        totalThreadsRegistered++;
        mtx.unlock();
        workerThreadFunction();
    } else if (totalThreadsRegistered > config.maxThreadsForExecution) {
        mtx.unlock();
        return;
    } else {
        masterIsSelected = true;
        mtx.unlock();
        compute();
    }
}

void Algorithm::workerThreadFunction() {
    std::unique_lock<std::mutex> lck{mtx, std::defer_lock};
    AlgorithmTask* algorithmTask;
    while (isActive) {
        lck.lock();
        cv.wait(lck, [&] {
            if (!pendingAlgorithmTasks.empty()) {
                // get the job
                algorithmTask = getJobFromQueue();
            }
            return algorithmTask || !isActive;
        });
        lck.unlock();
        if (!isActive) {
            return;
        }
        try {
            // execute
            if(algorithmTask->getTaskType() == Algorithm_Task_Type::BELLMAN_FORD) {

            } else {

            }
        } catch (std::exception& e) {
            isActive.store(false);
            continue;
        }
    }
}

// Should be overriden by each Algorithm class that inherits
// Either just return the queue front, or pick the best one.
AlgorithmTask* Algorithm::getJobFromQueue() {
    AlgorithmTask*ret;
    auto it = pendingAlgorithmTasks.begin();
    while (it != pendingAlgorithmTasks.end()) {
        // iterate over all jobs to check which one is most suitable, important for
        // graph specific algorithms, such as for shortest path --> max[frontier_size / threads]
        // specific algorithms will override this
    }
    return ret;
}

void Algorithm::compute() {
    addTasksToQueue();
}

/**
 * This is an example, all graph algorithms will override this.
 * The pending tasks queue needs to be populated from the main thread with enough amount of work
 * upto whatever is the max configured level.
 */
void Algorithm::addTasksToQueue() {
    std::unique_lock lck{mtx};
    for(auto i = 0u; i < config.maxActiveTasks; i++) {
        auto newTask = std::make_unique<BellmanFordTask>(0, 30, 100000);
        pendingAlgorithmTasks.emplace_back(std::move(newTask));
    }
}

}
}