#include "function/algorithm/graph_algorithms.h"
#include "processor/operator/algorithm/algorithm_runner_main.h"

using namespace kuzu::processor;

namespace kuzu {
namespace processor {

void AlgorithmRunnerMain::initGlobalStateInternal(ExecutionContext*) {
    function::TableFunctionInitInput tableFunctionInitInput{info.bindData.get()};
    sharedState->funcState = info.function.initSharedStateFunc(tableFunctionInitInput);
}

void AlgorithmRunnerMain::executeInternal(ExecutionContext* executionContext) {
    graphAlgorithm->compute(executionContext);
}

} // namespace processor
} // namespace kuzu
