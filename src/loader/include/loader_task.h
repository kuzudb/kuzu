#pragma once

#include "src/common/include/task_system/task.h"

namespace graphflow {
namespace loader {

class LoaderTask : public Task {
public:
    // Loader tasks are intended to be tasks on which only 1 thread works.
    LoaderTask() : Task(1){};
};

template<typename F>
class ParameterizedLoaderTask : public LoaderTask {
public:
    ParameterizedLoaderTask(F&& func) : f(func){};
    void run() override { f(); }

private:
    F f;
};

class LoaderTaskFactory {

private:
    template<typename F>
    static shared_ptr<LoaderTask> createLoaderTaskInternal(F&& f) {
        return std::shared_ptr<LoaderTask>(new ParameterizedLoaderTask<F>(std::forward<F>(f)));
    };

public:
    template<typename F, typename... Args>
    static shared_ptr<LoaderTask> createLoaderTask(F function, Args&&... args) {
        return std::shared_ptr<LoaderTask>(createLoaderTaskInternal(move(bind(function, args...))));
    };
};
} // namespace loader
} // namespace graphflow
