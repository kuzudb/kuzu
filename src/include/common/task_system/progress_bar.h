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

    void finishPipeline(uint64_t queryID);

    void endProgress(uint64_t queryID);

    void startProgress(uint64_t queryID);

    void toggleProgressBarPrinting(bool enable);

    void setShowProgressAfter(uint64_t showProgressAfter);

    void updateProgress(uint64_t queryID, double curPipelineProgress);

    void setDisplay(std::shared_ptr<ProgressBarDisplay> progressBarDipslay);

    std::shared_ptr<ProgressBarDisplay> getDisplay() { return display; }

    bool getProgressBarPrinting() const { return trackProgress; }

private:
    void resetProgressBar(uint64_t queryID);

    void updateDisplay(uint64_t queryID, double curPipelineProgress);

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
