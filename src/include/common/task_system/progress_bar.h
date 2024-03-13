#pragma once

#include <algorithm>
#include <iostream>
#include <mutex>

namespace kuzu {
namespace common {

/**
 * TODO: PUT DESCRIPTION HERE
 */
class ProgressBar {

public:
    ProgressBar()
        : numPipelines{0}, numPipelinesFinished{0}, currentPipelineJobs{0},
          currentPipelineJobsFinished{0}, trackProgress{false} {};

    void addPipeline();

    void finishPipeline();

    void addJobsToPipeline(int jobs);

    void finishJobsInPipeline(int jobs);

    void startProgress();

    void toggleProgressBarPrinting(bool enable);

private:
    inline void setGreenFont() const { std::cerr << "\033[1;32m"; }

    inline void setDefaultFont() const { std::cerr << "\033[0m"; }

    void printProgressBar() const;

    void finishTask() const;

    void resetProgressBar();

private:
    int numPipelines;
    int numPipelinesFinished;
    int currentPipelineJobs;
    int currentPipelineJobsFinished;
    std::mutex progressBarLock;
    bool trackProgress;
};

} // namespace common
} // namespace kuzu
