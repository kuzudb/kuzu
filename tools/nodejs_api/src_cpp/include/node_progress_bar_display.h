#pragma once

#include <shared_mutex>

#include "common/task_system/progress_bar_display.h"
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

    void setCallbackFunction(uint64_t queryID, Napi::ThreadSafeFunction callback);

    uint32_t getNumCallbacks() { return queryCallbacks.size(); }

private:
    std::unordered_map<uint64_t, Napi::ThreadSafeFunction> queryCallbacks;
    std::shared_mutex callbackMutex;
};
