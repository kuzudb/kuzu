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

void ProgressBar::startProgress(uint64_t queryID) {
    if (!trackProgress) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    queryTimer->start();
    updateDisplay(queryID, 0.0);
}

void ProgressBar::endProgress(uint64_t queryID) {
    std::lock_guard<std::mutex> lock(progressBarLock);
    resetProgressBar(queryID);
}

void ProgressBar::addPipeline() {
    if (!trackProgress) {
        return;
    }
    numPipelines++;
    display->setNumPipelines(numPipelines);
}

void ProgressBar::finishPipeline(uint64_t queryID) {
    if (!trackProgress) {
        return;
    }
    numPipelinesFinished++;
    updateProgress(queryID, 0.0);
}

void ProgressBar::updateProgress(uint64_t queryID, double curPipelineProgress) {
    if (!trackProgress) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    updateDisplay(queryID, curPipelineProgress);
}

void ProgressBar::resetProgressBar(uint64_t queryID) {
    numPipelines = 0;
    numPipelinesFinished = 0;
    if (queryTimer->isStarted) {
        queryTimer->stop();
    }
    display->finishProgress(queryID);
}

bool ProgressBar::shouldUpdateProgress() const {
    if (queryTimer->isStarted) {
        queryTimer->stop();
    }
    bool shouldUpdate = queryTimer->getElapsedTimeMS() > showProgressAfter;
    queryTimer->start();
    return shouldUpdate;
}

void ProgressBar::updateDisplay(uint64_t queryID, double curPipelineProgress) {
    if (shouldUpdateProgress()) {
        display->updateProgress(queryID, curPipelineProgress, numPipelinesFinished);
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
