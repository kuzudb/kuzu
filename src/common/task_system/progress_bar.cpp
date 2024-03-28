#include "common/task_system/progress_bar.h"

namespace kuzu {
namespace common {

void ProgressBar::startProgress() {
    if (!trackProgress) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    queryTimer->start();
    printProgressBar(0.0);
}

void ProgressBar::endProgress() {
    std::lock_guard<std::mutex> lock(progressBarLock);
    resetProgressBar();
    queryTimer = std::make_unique<TimeMetric>(true);
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
    if (printing) {
        std::cout << "\033[1A\033[2K\033[1B";
    }
    // This ensures that the progress bar is updated back to 0% after a pipeline is finished.
    prevCurPipelineProgress = -0.01;
    updateProgress(0.0);
}

void ProgressBar::updateProgress(double curPipelineProgress) {
    if (!trackProgress) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    // Only update the progress bar if the progress has changed by at least 1%.
    if (curPipelineProgress - prevCurPipelineProgress < 0.01) {
        return;
    }
    prevCurPipelineProgress = curPipelineProgress;
    if (printing) {
        std::cout << "\033[2A";
    }
    printProgressBar(curPipelineProgress);
}

void ProgressBar::printProgressBar(double curPipelineProgress) {
    if (!shouldPrintProgress()) {
        return;
    }
    printing = true;
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
    printing = false;
    if (queryTimer->isStarted) {
        queryTimer->stop();
    }
}

bool ProgressBar::shouldPrintProgress() const {
    if (queryTimer->isStarted) {
        queryTimer->stop();
    }
    bool shouldPrint = queryTimer->getElapsedTimeMS() > showProgressAfter;
    queryTimer->start();
    return shouldPrint;
}

void ProgressBar::toggleProgressBarPrinting(bool enable) {
    trackProgress = enable;
}

void ProgressBar::setShowProgressAfter(uint64_t time) {
    showProgressAfter = time;
}

} // namespace common
} // namespace kuzu
