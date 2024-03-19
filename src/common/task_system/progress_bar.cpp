#include "common/task_system/progress_bar.h"

namespace kuzu {
namespace common {

void ProgressBar::startProgress() {
    if (!trackProgress) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    printProgressBar(0.0);
}

void ProgressBar::endProgress() {
    if (!trackProgress) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    resetProgressBar();
}

void ProgressBar::addPipeline() {
    if (!trackProgress) {
        return;
    }
    numPipelines++;
}

void ProgressBar::finishPipeline() {
    if (!trackProgress) {
        return;
    }
    numPipelinesFinished++;
    // allow progress to print 0% for the next pipeline
    prevCurPipelineProgress = -0.01;
    updateProgress(0.0);
}

// to mitigate unnecessary progress bar updates, we only update the progress bar when the progress
// changes by >= 1%
void ProgressBar::updateProgress(double curPipelineProgress) {
    if (!trackProgress || curPipelineProgress - prevCurPipelineProgress < 0.01) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    prevCurPipelineProgress = curPipelineProgress;
    std::cout << "\033[2A\033[2K\033[1B\033[2K\033[1A";
    printProgressBar(curPipelineProgress);
}

void ProgressBar::printProgressBar(double curPipelineProgress) const {
    float pipelineProgress = 0.0;
    if (numPipelines > 0) {
        pipelineProgress = (float)numPipelinesFinished / (float)numPipelines;
    }
    setGreenFont();
    std::cout << "Pipelines Finished: " << int(pipelineProgress * 100.0) << "%"
              << "\n";
    std::cout << "Current Pipeline Progress: " << int(curPipelineProgress * 100.0) << "%"
              << "\n";
    std::cout.flush();
    setDefaultFont();
}

void ProgressBar::resetProgressBar() {
    std::cout << "\033[2A\033[2K\033[1B\033[2K\033[1A";
    std::cout.flush();
    numPipelines = 0;
    numPipelinesFinished = 0;
    prevCurPipelineProgress = 0.0;
}

void ProgressBar::toggleProgressBarPrinting(bool enable) {
    trackProgress = enable;
}

} // namespace common
} // namespace kuzu
