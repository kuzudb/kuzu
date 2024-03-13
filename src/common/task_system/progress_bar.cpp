#include "common/task_system/progress_bar.h"

namespace kuzu {
namespace common {

void ProgressBar::startProgress() {
    if (!trackProgress) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    printProgressBar();
}

void ProgressBar::addPipeline() {
    if (!trackProgress) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    numPipelines++;
}

void ProgressBar::finishPipeline() {
    if (!trackProgress) {
        return;
    }
    std::lock_guard<std::mutex> lock(progressBarLock);
    numPipelinesFinished++;
    currentPipelineJobsFinished = 0;
    currentPipelineJobs = 0;
    finishTask();
    if (numPipelines == numPipelinesFinished) {
        resetProgressBar();
    }
}

void ProgressBar::addJobsToPipeline(int jobs) {
    if (!trackProgress) {
        return;
    }
	std::lock_guard<std::mutex> lock(progressBarLock);
    if (currentPipelineJobs == 0) {
		currentPipelineJobs = jobs;
	}
}

void ProgressBar::finishJobsInPipeline(int jobs) {
    if (!trackProgress) {
        return;
    }
	std::lock_guard<std::mutex> lock(progressBarLock);
	currentPipelineJobsFinished += jobs;
    finishTask();
}

void ProgressBar::finishTask() const {
    std::cout << "\033[2A\033[2K\033[1B\033[2K\033[1A";
    printProgressBar();
}

void ProgressBar::printProgressBar() const {
    float pipelineProgress = 0.0;
    if (numPipelines > 0) {
        pipelineProgress = (float)numPipelinesFinished / (float)numPipelines;
    }
    float currentPipelineProgress = 0.0;
    if (currentPipelineJobs > 0) {
        currentPipelineProgress = (float)currentPipelineJobsFinished / (float)currentPipelineJobs;
    }
    setGreenFont();
    std::cout << "Pipelines Finished: " << int(pipelineProgress * 100.0) << "%"
         << "\n";
    std::cout << "Current Pipeline Progress: " << int(currentPipelineProgress * 100.0) << "%" 
         << "\n";
    std::cout.flush();
    setDefaultFont();
}

void ProgressBar::resetProgressBar() {
    std::cout << "\033[2A\033[2K\033[1B\033[2K\033[1A";
    std::cout.flush();
    numPipelines = 0;
    numPipelinesFinished = 0;
}

void ProgressBar::toggleProgressBarPrinting(bool enable) {
    trackProgress = enable;
}

} // namespace common
} // namespace kuzu
