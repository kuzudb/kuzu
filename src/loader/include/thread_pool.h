#pragma once

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

using namespace std;

namespace graphflow {
namespace common {

class TaskContainerBase {

public:
    virtual ~TaskContainerBase(){};
    virtual void operator()() = 0;
};

template<typename F>
class TaskContainer : public TaskContainerBase {
public:
    TaskContainer(F&& func) : f(func){};
    void operator()() override { f(); };

private:
    F f;
};

class ThreadPool {

public:
    ThreadPool(uint32_t threadCount);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    template<typename F, typename... Args>
    void execute(F function, Args&&... args);

    void wait();

private:
    // condition_variable& tasksQueueCV,
    static void threadInstance(queue<exception_ptr>& exceptionsQueue, uint32_t& numThreadsRunning,
        queue<unique_ptr<TaskContainerBase>>& tasks, mutex& tasksQueueMutex, bool& stopThreads);

    static unique_ptr<TaskContainerBase> getTask(queue<unique_ptr<TaskContainerBase>>& tasks,
        mutex& tasksQueueMutex, uint32_t& numThreadsRunning);

    template<typename F>
    inline static unique_ptr<TaskContainerBase> CreateNewTaskContainer(F&& f) {
        return std::unique_ptr<TaskContainerBase>(new TaskContainer<F>(std::forward<F>(f)));
    };

private:
    vector<thread> threads;
    queue<std::exception_ptr> exceptionsQueue;
    uint32_t numThreadsRunning = 0;
    queue<unique_ptr<TaskContainerBase>> tasks;
    mutex tasksQueueMutex;
    bool stopThreads = false;
};

template<typename F, typename... Args>
void ThreadPool::execute(F function, Args&&... args) {
    unique_lock<mutex> lock(tasksQueueMutex, defer_lock);
    lock.lock();
    tasks.emplace(CreateNewTaskContainer(move(bind(function, args...))));
    lock.unlock();
};

} // namespace common
} // namespace graphflow
