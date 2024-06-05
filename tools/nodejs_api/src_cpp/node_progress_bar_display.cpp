#include "include/node_progress_bar_display.h"

using namespace kuzu;
using namespace common;

void NodeProgressBarDisplay::updateProgress(double newPipelineProgress,
    uint32_t newNumPipelinesFinished) {
    uint32_t progress = (uint32_t)(newPipelineProgress * 100.0);
    uint32_t oldProgress = (uint32_t)(pipelineProgress * 100.0);
    if (progress > oldProgress || newNumPipelinesFinished > numPipelinesFinished) {
        pipelineProgress = newPipelineProgress;
        numPipelinesFinished = newNumPipelinesFinished;
        callback.BlockingCall([this](Napi::Env env, Napi::Function jsCallback) {
            jsCallback.Call({Napi::Number::New(env, pipelineProgress),
                Napi::Number::New(env, numPipelinesFinished),
                Napi::Number::New(env, numPipelines)});
        });
    }
}

void NodeProgressBarDisplay::finishProgress() {
    pipelineProgress = 0;
    numPipelines = 0;
    numPipelinesFinished = 0;
}
