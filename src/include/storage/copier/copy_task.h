#pragma once

#include "common/task_system/task.h"

namespace kuzu {
namespace storage {

class CopyTask : public common::Task {
public:
    // CopyCSV tasks are intended to be tasks on which only 1 thread works.
    CopyTask() : Task(1){};
};

template<typename F>
class ParameterizedCopyTask : public CopyTask {
public:
    ParameterizedCopyTask(F&& func) : f(func){};
    void run() override { f(); }

private:
    F f;
};

class CopyTaskFactory {

private:
    template<typename F>
    static std::shared_ptr<CopyTask> createCopyTaskInternal(F&& f) {
        return std::shared_ptr<CopyTask>(new ParameterizedCopyTask<F>(std::forward<F>(f)));
    };

public:
    template<typename F, typename... Args>
    static std::shared_ptr<CopyTask> createCopyTask(F function, Args&&... args) {
        return std::shared_ptr<CopyTask>(
            createCopyTaskInternal(std::move(std::bind(function, args...))));
    };
};
} // namespace storage
} // namespace kuzu
