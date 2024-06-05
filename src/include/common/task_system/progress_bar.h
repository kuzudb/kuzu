#pragma once

#include <memory>
#include <mutex>

#include "common/metric.h"
#include "progress_bar_display.h"

namespace kuzu {
namespace common {

typedef std::unique_ptr<ProgressBarDisplay> (*progress_bar_display_create_func_t)();

/**
 * @brief Progress bar for tracking the progress of a pipeline. Prints the progress of each query
 * pipeline and the overall progress.
 */
class ProgressBar {
public:
    ProgressBar();

    static std::shared_ptr<ProgressBarDisplay> DefaultProgressBarDisplay();

    void addPipeline();

    void finishPipeline();

    void endProgress();

    void addJobsToPipeline(int jobs);

    void finishJobsInPipeline(int jobs);

    void startProgress();

    void toggleProgressBarPrinting(bool enable);

    void setShowProgressAfter(uint64_t showProgressAfter);

    void updateProgress(double curPipelineProgress);

    void setDisplay(std::shared_ptr<ProgressBarDisplay> progressBarDipslay);

    bool getProgressBarPrinting() const { return trackProgress; }

private:
    void resetProgressBar();

    void updateDisplay(double curPipelineProgress);

    bool shouldUpdateProgress() const;

private:
    uint32_t numPipelines;
    uint32_t numPipelinesFinished;
    std::mutex progressBarLock;
    bool trackProgress;
    std::unique_ptr<TimeMetric> queryTimer;
    uint64_t showProgressAfter;
    std::shared_ptr<ProgressBarDisplay> display;
};

} // namespace common
} // namespace kuzu
