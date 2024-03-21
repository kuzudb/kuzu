#include "common/task_system/progress_bar.h"

namespace kuzu {
namespace common {

void ProgressBar::startProgress() {
    if (!trackProgress) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    printProgressBar(0.0);
    printing = true;
}

void ProgressBar::endProgress() {
    std::lock_guard<std::mutex> lock(progressBarLock);
    resetProgressBar();
    printing = false;
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
    // This ensures that the progress bar is updated back to 0% after a pipeline is finished.
    prevCurPipelineProgress = -0.01;
    updateProgress(0.0);
}

void ProgressBar::updateProgress(double curPipelineProgress) {
    // Only update the progress bar if the progress has changed by at least 1%.
    if (!trackProgress || curPipelineProgress - prevCurPipelineProgress < 0.01) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    prevCurPipelineProgress = curPipelineProgress;
    if (printing) {
        std::cout << "\033[2A";
    }
    printProgressBar(curPipelineProgress);
    printing = true;
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
    if (printing) {
        std::cout << "\033[2A\033[2K\033[1B\033[2K\033[1A";
        std::cout.flush();
    }
    numPipelines = 0;
    numPipelinesFinished = 0;
    prevCurPipelineProgress = 0.0;
}

void ProgressBar::toggleProgressBarPrinting(bool enable) {
    trackProgress = enable;
}

} // namespace common
} // namespace kuzu
