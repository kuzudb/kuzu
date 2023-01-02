#pragma once

#include "common/task_system/task.h"

namespace kuzu {
namespace storage {

class CopyCSVTask : public Task {
public:
    // CopyCSV tasks are intended to be tasks on which only 1 thread works.
    CopyCSVTask() : Task(1){};
};

template<typename F>
class ParameterizedCopyCSVTask : public CopyCSVTask {
public:
    ParameterizedCopyCSVTask(F&& func) : f(func){};
    void run() override { f(); }

private:
    F f;
};

class CopyCSVTaskFactory {

private:
    template<typename F>
    static shared_ptr<CopyCSVTask> createCopyCSVTaskInternal(F&& f) {
        return std::shared_ptr<CopyCSVTask>(new ParameterizedCopyCSVTask<F>(std::forward<F>(f)));
    };

public:
    template<typename F, typename... Args>
    static shared_ptr<CopyCSVTask> createCopyCSVTask(F function, Args&&... args) {
        return std::shared_ptr<CopyCSVTask>(
            createCopyCSVTaskInternal(move(bind(function, args...))));
    };
};
} // namespace storage
} // namespace kuzu
