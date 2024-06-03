#include "common/task_system/terminal_progress_bar_display.h"

namespace kuzu {
namespace common {

void TerminalProgressBarDisplay::updateProgress(double newPipelineProgress,
    uint32_t newNumPipelinesFinished) {
    uint32_t progress = (uint32_t)(newPipelineProgress * 100.0);
    uint32_t oldProgress = (uint32_t)(pipelineProgress * 100.0);
    if (progress > oldProgress || newNumPipelinesFinished > numPipelinesFinished) {
        pipelineProgress = newPipelineProgress;
        numPipelinesFinished = newNumPipelinesFinished;
        printProgressBar();
    }
}

void TerminalProgressBarDisplay::finishProgress() {
    if (printing) {
        std::cout << "\033[2A\033[2K\033[1B\033[2K\033[1A";
        std::cout.flush();
    }
    printing = false;
    pipelineProgress = 0;
    numPipelines = 0;
    numPipelinesFinished = 0;
}

void TerminalProgressBarDisplay::printProgressBar() {
    setGreenFont();
    if (printing) {
        if (pipelineProgress == 0) {
            std::cout << "\033[1A\033[2K\033[1A";
            printing = false;
        } else {
            std::cout << "\033[1A";
        }
    }
    if (!printing) {
        std::cout << "Pipelines Finished: " << numPipelinesFinished << "/" << numPipelines << "\n";
        printing = true;
    }
    std::cout << "Current Pipeline Progress: " << uint32_t(pipelineProgress * 100.0) << "%"
              << "\n";
    std::cout.flush();
    setDefaultFont();
}

} // namespace common
} // namespace kuzu
