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

void ProgressBar::startProgress(std::string id) {
    if (!trackProgress) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    queryTimer->start();
    updateDisplay(id, 0.0);
}

void ProgressBar::endProgress(std::string id) {
    std::lock_guard<std::mutex> lock(progressBarLock);
    resetProgressBar(id);
}

void ProgressBar::addPipeline() {
    if (!trackProgress) {
        return;
    }
    numPipelines++;
    display->setNumPipelines(numPipelines);
}

void ProgressBar::finishPipeline(std::string id) {
    if (!trackProgress) {
        return;
    }
    numPipelinesFinished++;
    updateProgress(id, 0.0);
}

void ProgressBar::updateProgress(std::string id, double curPipelineProgress) {
    if (!trackProgress) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    updateDisplay(id, curPipelineProgress);
}

void ProgressBar::resetProgressBar(std::string id) {
    numPipelines = 0;
    numPipelinesFinished = 0;
    if (queryTimer->isStarted) {
        queryTimer->stop();
    }
    display->finishProgress(id);
}

bool ProgressBar::shouldUpdateProgress() const {
    if (queryTimer->isStarted) {
        queryTimer->stop();
    }
    bool shouldUpdate = queryTimer->getElapsedTimeMS() > showProgressAfter;
    queryTimer->start();
    return shouldUpdate;
}

void ProgressBar::updateDisplay(std::string id, double curPipelineProgress) {
    if (shouldUpdateProgress()) {
        display->updateProgress(id, curPipelineProgress, numPipelinesFinished);
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
