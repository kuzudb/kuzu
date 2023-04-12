#pragma once

#include "common/task_system/task.h"

namespace kuzu {
namespace storage {

class CopyTask : public common::Task {
public:
    CopyTask(uint64_t numThreads) : Task(numThreads){};
};

template<typename F>
class ParameterizedCopyTask : public CopyTask {
public:
    ParameterizedCopyTask(F&& func, uint64_t numThreads = 1) : CopyTask(numThreads), f(func){};
    void run() override { f(); }

private:
    F f;
};

class CopyTaskFactory {

private:
    template<typename F>
    static std::shared_ptr<CopyTask> createCopyTaskInternal(F&& f, uint64_t numThreads = 1) {
        return std::shared_ptr<CopyTask>(
            new ParameterizedCopyTask<F>(std::forward<F>(f), numThreads));
    };

public:
    template<typename F, typename... Args>
    static std::shared_ptr<CopyTask> createCopyTask(F function, Args&&... args) {
        return createParallelCopyTask(1 /* num threads */, function, args...);
    };

    template<typename F, typename... Args>
    static std::shared_ptr<CopyTask> createParallelCopyTask(
        uint64_t numThreads, F function, Args&&... args) {
        return createCopyTaskInternal(std::move(std::bind(function, args...)), numThreads);
    };
};
} // namespace storage
} // namespace kuzu
