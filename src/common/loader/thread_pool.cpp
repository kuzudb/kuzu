#include "src/common/loader/include/thread_pool.h"

#include <thread>

namespace graphflow {
namespace common {

ThreadPool::ThreadPool(uint32_t threadCount) {
    for (size_t i = 0; i < threadCount; ++i) {
        threads.emplace_back(thread(threadInstance, ref(numThreadsRunning), ref(tasks),
            ref(tasksQueueMutex), ref(tasksQueueCV), ref(stopThreads)));
    }
}

ThreadPool::~ThreadPool() {
    stopThreads = true;
    tasksQueueCV.notify_all();

    for (std::thread& thread : threads) {
        thread.join();
    }
}

void ThreadPool::threadInstance(atomic<uint32_t>& numThreadsRunning,
    queue<unique_ptr<TaskContainerBase>>& tasks, mutex& tasksQueueMutex,
    condition_variable& tasksQueueCV, bool& stopThreads) {
    unique_lock<mutex> tasksQueueLock(tasksQueueMutex, defer_lock);
    while (true) {
        tasksQueueLock.lock();
        tasksQueueCV.wait(tasksQueueLock, [&]() -> bool { return !tasks.empty() || stopThreads; });
        if (stopThreads && tasks.empty()) {
            return;
        }
        auto toExecute = move(tasks.front());
        tasks.pop();
        tasksQueueLock.unlock();
        numThreadsRunning.fetch_add(1);
        (*toExecute)();
        numThreadsRunning.fetch_sub(1);
    }
}

void ThreadPool::wait() {
    while (!tasks.empty() || 0 != numThreadsRunning.load()) {
        this_thread::sleep_for(chrono::milliseconds(10));
    }
}

} // namespace common
} // namespace graphflow
