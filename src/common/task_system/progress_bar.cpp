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
    prevCurPipelineProgress = 0.0;
    updateProgress(0.0);
    if (numPipelines == numPipelinesFinished) {
        resetProgressBar();
    }
}

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
    std::lock_guard<std::mutex> lock(progressBarLock);
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
