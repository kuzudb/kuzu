#pragma once

#include <optional>
#include <unordered_set>

#include "common/task_system/progress_bar_display.h"
#include "common/task_system/terminal_progress_bar_display.h"
#include <napi.h>

using namespace kuzu;
using namespace common;

/**
 * @brief A class that displays a progress bar in the terminal.
 */
class NodeProgressBarDisplay : public ProgressBarDisplay {
public:
    void updateProgress(uint64_t queryID, double newPipelineProgress,
        uint32_t newNumPipelinesFinished) override;

    void finishProgress(uint64_t queryID) override;

    void setCallbackFunction(uint64_t queryID, Napi::ThreadSafeFunction callback, Napi::Env env);

    uint32_t getNumCallbacks() { return queryCallbacks.size(); }

private:
    void printProgressBar();

private:
    struct callbackFunction {
        Napi::ThreadSafeFunction callback;
        Napi::Env env;
    };

    bool printing = false;
    std::unordered_map<uint64_t, callbackFunction> queryCallbacks;
};
