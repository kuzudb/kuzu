#include "include/node_progress_bar_display.h"

using namespace kuzu;
using namespace common;

void NodeProgressBarDisplay::updateProgress(uint64_t queryID, double newPipelineProgress,
    uint32_t newNumPipelinesFinished) {
    if (numPipelines == 0) {
        return;
    }
    uint32_t curPipelineProgress = (uint32_t)(newPipelineProgress * 100.0);
    uint32_t oldPipelineProgress = (uint32_t)(pipelineProgress * 100.0);
    if (curPipelineProgress > oldPipelineProgress ||
        newNumPipelinesFinished > numPipelinesFinished) {
        pipelineProgress = newPipelineProgress;
        numPipelinesFinished = newNumPipelinesFinished;
        auto callback = queryCallbacks.find(queryID);
        if (callback != queryCallbacks.end()) {
            callback->second.callback.BlockingCall(
                [this, callback](Napi::Env /*env*/, Napi::Function jsCallback) {
                    jsCallback.Call({Napi::Number::New(callback->second.env, pipelineProgress),
                        Napi::Number::New(callback->second.env, numPipelinesFinished),
                        Napi::Number::New(callback->second.env, numPipelines)});
                });
        } else {
            printProgressBar();
        }
    }
}

void NodeProgressBarDisplay::finishProgress(uint64_t queryID) {
    if (printing) {
        std::cout << "\033[2A\033[2K\033[1B\033[2K\033[1A";
        std::cout.flush();
        printing = false;
    }
    numPipelines = 0;
    numPipelinesFinished = 0;
    pipelineProgress = 0;
    queryCallbacks.erase(queryID);
}

void NodeProgressBarDisplay::setCallbackFunction(uint64_t queryID,
    Napi::ThreadSafeFunction callback, Napi::Env env) {
    queryCallbacks.emplace(queryID, callbackFunction{callback, env});
}

void NodeProgressBarDisplay::printProgressBar() {
    std::cerr << "\033[1;32m";
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
    std::cerr << "\033[0m";
    std::cout.flush();
}
