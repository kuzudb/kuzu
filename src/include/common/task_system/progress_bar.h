#pragma once

#include <iostream>
#include <mutex>

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
          trackProgress{false}, printing{false} {};

    void addPipeline();

    void finishPipeline();

    void endProgress();

    void addJobsToPipeline(int jobs);

    void finishJobsInPipeline(int jobs);

    void startProgress();

    void toggleProgressBarPrinting(bool enable);

    void updateProgress(double curPipelineProgress);

private:
    inline void setGreenFont() const { std::cerr << "\033[1;32m"; }

    inline void setDefaultFont() const { std::cerr << "\033[0m"; }

    void printProgressBar(double curPipelineProgress) const;

    void resetProgressBar();

private:
    uint32_t numPipelines;
    uint32_t numPipelinesFinished;
    double prevCurPipelineProgress;
    std::mutex progressBarLock;
    bool trackProgress;
    bool printing;
};

} // namespace common
} // namespace kuzu
