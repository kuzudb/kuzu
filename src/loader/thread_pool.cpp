#include "src/loader/include/thread_pool.h"

#include <thread>

namespace graphflow {
namespace common {

ThreadPool::ThreadPool(uint32_t threadCount) {
    for (size_t i = 0; i < threadCount; ++i) {
        exceptionPtrs.emplace_back(std::exception_ptr());
        threads.emplace_back(thread(threadInstance, &exceptionPtrs[i], ref(numThreadsRunning),
            ref(tasks), ref(tasksQueueMutex), ref(tasksQueueCV), ref(stopThreads)));
    }
}

ThreadPool::~ThreadPool() {
    stopThreads = true;
    tasksQueueCV.notify_all();
    for (std::thread& thread : threads) {
        thread.join();
    }
}

void ThreadPool::threadInstance(exception_ptr* exceptionPtrLoc, atomic<uint32_t>& numThreadsRunning,
    queue<unique_ptr<TaskContainerBase>>& tasks, mutex& tasksQueueMutex,
    condition_variable& tasksQueueCV, bool& stopThreads) {
    unique_lock<mutex> tasksQueueLock(tasksQueueMutex, defer_lock);
    bool exceptionRaised = false;
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
        try {
            (*toExecute)();
        } catch (exception& e) {
            *exceptionPtrLoc = current_exception();
            exceptionRaised = true;
        }
        numThreadsRunning.fetch_sub(1);
        if (exceptionRaised) {
            break;
        }
    }
}

void ThreadPool::wait() {
    while (!tasks.empty() || 0 != numThreadsRunning.load()) {
        this_thread::sleep_for(chrono::milliseconds(10));
    }
    for (auto& exceptionPtr : exceptionPtrs) {
        if (exceptionPtr) {
            std::rethrow_exception(exceptionPtr);
        }
    }
}

} // namespace common
} // namespace graphflow
