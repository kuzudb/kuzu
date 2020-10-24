#ifndef GRAPHFLOW_COMMON_LOADER_THREAD_POOL_H_
#define GRAPHFLOW_COMMON_LOADER_THREAD_POOL_H_

#include <condition_variable>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

using namespace std;

namespace graphflow {
namespace common {

class ThreadPool {

    class TaskContainerBase;

public:
    ThreadPool(uint32_t threadCount);
    ~ThreadPool();

    ThreadPool(const ThreadPool &) = delete;
    ThreadPool &operator=(const ThreadPool &) = delete;

    template<typename F, typename... Args>
    void execute(F function, Args &&... args);

    void wait();

private:
    static void threadInstance(atomic<uint32_t> &numThreadsRunning,
        queue<unique_ptr<TaskContainerBase>> &tasks, mutex &tasksQueueMutex,
        condition_variable &tasksQueueCV, bool &stopThreads);
    template<typename F>
    inline static unique_ptr<TaskContainerBase> CreateNewTaskContainer(F &&f) {
        return std::unique_ptr<TaskContainerBase>(new TaskContainer<F>(std::forward<F>(f)));
    };

private:
    class TaskContainerBase {

    public:
        virtual ~TaskContainerBase(){};
        virtual void operator()() = 0;
    };

    template<typename F>
    class TaskContainer : public TaskContainerBase {

    public:
        TaskContainer(F &&func) : f(forward<F>(func)){};
        void operator()() override { f(); };

    private:
        F f;
    };

    vector<thread> threads;
    atomic<uint32_t> numThreadsRunning = 0;
    queue<unique_ptr<TaskContainerBase>> tasks;
    mutex tasksQueueMutex;
    condition_variable tasksQueueCV;
    bool stopThreads = false;
};

template<typename F, typename... Args>
void ThreadPool::execute(F function, Args &&... args) {
    unique_lock<mutex> lock(tasksQueueMutex, defer_lock);
    packaged_task<invoke_result_t<F, Args...>()> taskPackage(bind(function, args...));
    future<invoke_result_t<F, Args...>> future = taskPackage.get_future();
    lock.lock();
    tasks.emplace(CreateNewTaskContainer(std::move(taskPackage)));
    lock.unlock();
    tasksQueueCV.notify_one();
};

} // namespace common
} // namespace graphflow

#endif // GRAPHFLOW_COMMON_LOADER_THREAD_POOL_H_