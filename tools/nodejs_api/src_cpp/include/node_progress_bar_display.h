#pragma once

#include "common/task_system/progress_bar_display.h"
#include <napi.h>

using namespace kuzu;
using namespace common;

/**
 * @brief A class that displays a progress bar in the terminal.
 */
class NodeProgressBarDisplay : public ProgressBarDisplay {
public:
    NodeProgressBarDisplay(Napi::ThreadSafeFunction callback, Napi::Env env)
        : callback(callback), env(env) {}

    void updateProgress(double newPipelineProgress, uint32_t newNumPipelinesFinished) override;

    void finishProgress() override;

private:
    Napi::ThreadSafeFunction callback;
    Napi::Env env;
};
