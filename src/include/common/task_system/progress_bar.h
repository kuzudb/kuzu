#pragma once

#include <iostream>
#include <memory>
#include <mutex>

#include "common/metric.h"

namespace kuzu {
namespace common {

/**
 * @brief Progress bar for tracking the progress of a pipeline. Prints the progress of each query
 * pipeline and the overall progress.
 */
class ProgressBar {

public:
    ProgressBar()
        : numPipelines{0}, numPipelinesFinished{0}, prevCurPipelineProgress{0.0},
          trackProgress{false}, printing{false}, queryTimer{std::make_unique<TimeMetric>(true)},
          showProgressAfter{1000} {};

    void addPipeline();

    void finishPipeline();

    void endProgress();

    void addJobsToPipeline(int jobs);

    void finishJobsInPipeline(int jobs);

    void startProgress();

    void toggleProgressBarPrinting(bool enable);

    void setShowProgressAfter(uint64_t showProgressAfter);

    void updateProgress(double curPipelineProgress);

private:
    inline void setGreenFont() const { std::cerr << "\033[1;32m"; }

    inline void setDefaultFont() const { std::cerr << "\033[0m"; }

    void printProgressBar(double curPipelineProgress);

    void resetProgressBar();

    bool shouldPrintProgress() const;

private:
    uint32_t numPipelines;
    uint32_t numPipelinesFinished;
    double prevCurPipelineProgress;
    std::mutex progressBarLock;
    bool trackProgress;
    bool printing;
    std::unique_ptr<TimeMetric> queryTimer;
    uint64_t showProgressAfter;
};

} // namespace common
} // namespace kuzu
