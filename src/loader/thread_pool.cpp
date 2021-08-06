#include "src/loader/include/thread_pool.h"

#include <thread>

namespace graphflow {
namespace common {

ThreadPool::ThreadPool(uint32_t threadCount) {
    for (size_t i = 0; i < threadCount; ++i) {
        threads.emplace_back(thread(threadInstance, ref(exceptionsQueue), ref(numThreadsRunning),
            ref(tasks), ref(tasksQueueMutex), ref(stopThreads)));
    }
}

ThreadPool::~ThreadPool() {
    stopThreads = true;
    for (std::thread& thread : threads) {
        thread.join();
    }
}

unique_ptr<TaskContainerBase> ThreadPool::getTask(queue<unique_ptr<TaskContainerBase>>& tasks,
    mutex& tasksQueueMutex, uint32_t& numThreadsRunning) {
    unique_lock<mutex> tasksQueueLock(tasksQueueMutex);
    if (tasks.empty()) {
        return nullptr;
    }
    auto retVal = move(tasks.front());
    tasks.pop();
    numThreadsRunning++;
    return retVal;
}

// condition_variable& tasksQueueCV,
void ThreadPool::threadInstance(queue<exception_ptr>& exceptionsQueue, uint32_t& numThreadsRunning,
    queue<unique_ptr<TaskContainerBase>>& tasks, mutex& tasksQueueMutex, bool& stopThreads) {
    bool exceptionRaised = false;
    unique_ptr<TaskContainerBase> task;
    while (true) {
        if (stopThreads) {
            break;
        }
        task = getTask(tasks, tasksQueueMutex, numThreadsRunning);
        if (!task) {
            //            cout << "Thread " << std::this_thread::get_id()
            //                 << " is sleeping for 10000 micros in ThreadPool::threadInstance." <<
            //                 endl;
            this_thread::sleep_for(chrono::microseconds(10000));
            continue;
        }
        try {
            (*task)();
        } catch (exception& e) {
            unique_lock<mutex> tasksQueueLock(tasksQueueMutex);
            exceptionsQueue.push(current_exception());
            exceptionRaised = true;
            tasksQueueLock.unlock();
        }
        unique_lock<mutex> tasksQueueLock(tasksQueueMutex);
        numThreadsRunning--;
        tasksQueueLock.unlock();
        if (exceptionRaised) {
            break;
        }
    }
}

void ThreadPool::wait() {
    unique_lock<mutex> lock(tasksQueueMutex, defer_lock);
    while (true) {
        lock.lock();
        if (!tasks.empty() || 0 != numThreadsRunning) {
            lock.unlock();
            this_thread::sleep_for(chrono::milliseconds(10));
        } else {
            lock.unlock();
            break;
        }
    }

    lock.lock();
    if (!exceptionsQueue.empty()) {
        auto exceptionPtr = exceptionsQueue.front();
        exceptionsQueue.pop();
        lock.unlock();
        std::rethrow_exception(exceptionPtr);
    } else {
        lock.unlock();
    }
}

} // namespace common
} // namespace graphflow
