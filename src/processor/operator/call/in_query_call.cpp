#include "processor/operator/call/in_query_call.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void InQueryCall::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    for (auto& outputPos : inQueryCallInfo->outputPoses) {
        outputVectors.push_back(resultSet->getValueVector(outputPos).get());
    }
}

void InQueryCall::initGlobalStateInternal(ExecutionContext* /*context*/) {
    function::TableFunctionInitInput tableFunctionInitInput{inQueryCallInfo->bindData.get()};
    sharedState->sharedState =
        inQueryCallInfo->function->initSharedStateFunc(tableFunctionInitInput);
}

bool InQueryCall::getNextTuplesInternal(ExecutionContext* /*context*/) {
    function::TableFunctionInput tableFunctionInput{
        inQueryCallInfo->bindData.get(), sharedState->sharedState.get()};
    inQueryCallInfo->function->tableFunc(tableFunctionInput, outputVectors);
    return outputVectors[0]->state->selVector->selectedSize != 0;
}

} // namespace processor
} // namespace kuzu
