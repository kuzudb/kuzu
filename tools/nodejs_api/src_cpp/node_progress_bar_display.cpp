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
        pipelineProgress.store(newPipelineProgress);
        numPipelinesFinished.store(newNumPipelinesFinished);
        auto callback = queryCallbacks.find(queryID);
        if (callback != queryCallbacks.end()) {
            double capturedPipelineProgress = pipelineProgress;
            uint32_t capturedNumPipelinesFinished = numPipelinesFinished;
            uint32_t capturedNumPipelines = numPipelines;

            callback->second.BlockingCall(
                [capturedPipelineProgress, capturedNumPipelinesFinished,
                    capturedNumPipelines](Napi::Env env, Napi::Function jsCallback) {
                    // Use the captured values directly inside the lambda
                    jsCallback.Call({Napi::Number::New(env, capturedPipelineProgress),
                        Napi::Number::New(env, capturedNumPipelinesFinished),
                        Napi::Number::New(env, capturedNumPipelines)});
                });
        }
    }
}

void NodeProgressBarDisplay::finishProgress(uint64_t queryID) {
    numPipelines = 0;
    numPipelinesFinished = 0;
    pipelineProgress = 0;
    auto callback = queryCallbacks.find(queryID);
    if (callback != queryCallbacks.end()) {
        callback->second.Release();
    }
    queryCallbacks.erase(queryID);
}

void NodeProgressBarDisplay::setCallbackFunction(uint64_t queryID,
    Napi::ThreadSafeFunction callback) {
    queryCallbacks.emplace(queryID, callback);
}
