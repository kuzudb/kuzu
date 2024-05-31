#include "common/task_system/progress_bar.h"

#include "common/task_system/terminal_progress_bar_display.h"

namespace kuzu {
namespace common {

ProgressBar::ProgressBar() {
    display = DefaultProgressBarDisplay();
    numPipelines = 0;
    numPipelinesFinished = 0;
    queryTimer = std::make_unique<TimeMetric>(true);
    showProgressAfter = 1000;
    trackProgress = false;
}

std::shared_ptr<ProgressBarDisplay> ProgressBar::DefaultProgressBarDisplay() {
    return std::make_shared<TerminalProgressBarDisplay>();
}

void ProgressBar::setDisplay(std::shared_ptr<ProgressBarDisplay> progressBarDipslay) {
    display = progressBarDipslay;
}

void ProgressBar::startProgress() {
    if (!trackProgress) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    queryTimer->start();
    updateDisplay(0.0);
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
    display->setNumPipelines(numPipelines);
}

void ProgressBar::finishPipeline() {
    if (!trackProgress) {
        return;
    }
    numPipelinesFinished++;
    updateProgress(0.0);
}

void ProgressBar::updateProgress(double curPipelineProgress) {
    if (!trackProgress) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    updateDisplay(curPipelineProgress);
}

void ProgressBar::resetProgressBar() {
    numPipelines = 0;
    numPipelinesFinished = 0;
    if (queryTimer->isStarted) {
        queryTimer->stop();
    }
    display->finishProgress();
}

bool ProgressBar::shouldUpdateProgress() const {
    if (queryTimer->isStarted) {
        queryTimer->stop();
    }
    bool shouldUpdate = queryTimer->getElapsedTimeMS() > showProgressAfter;
    queryTimer->start();
    return shouldUpdate;
}

void ProgressBar::updateDisplay(double curPipelineProgress) {
    if (shouldUpdateProgress()) {
        display->updateProgress(curPipelineProgress, numPipelinesFinished);
    }
}

void ProgressBar::toggleProgressBarPrinting(bool enable) {
    trackProgress = enable;
}

void ProgressBar::setShowProgressAfter(uint64_t time) {
    showProgressAfter = time;
}

} // namespace common
} // namespace kuzu
