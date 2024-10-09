#include "processor/operator/table_function_call.h"

#include "binder/expression/expression_util.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string TableFunctionCallPrintInfo::toString() const {
    std::string result = "Function: ";
    result += funcName;
    return result;
}

std::string FTableScanFunctionCallPrintInfo::toString() const {
    std::string result = "Function: ";
    result += funcName;
    if (!exprs.empty()) {
        result += ", Expressions: ";
        result += binder::ExpressionUtil::toString(exprs);
    }
    return result;
}

void TableFunctionCall::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    // Init local state.
    localState = TableFunctionCallLocalState();
    // Init table function output.
    switch (info.outputType) {
    case TableScanOutputType::EMPTY:
        break; // Do nothing.
    case TableScanOutputType::SINGLE_DATA_CHUNK: {
        KU_ASSERT(!info.outPosV.empty());
        auto state = resultSet->getDataChunk(info.outPosV[0].dataChunkPos)->state;
        localState.funcOutput.dataChunk = DataChunk(info.outPosV.size(), state);
        for (auto i = 0u; i < info.outPosV.size(); ++i) {
            localState.funcOutput.dataChunk.insert(i, resultSet->getValueVector(info.outPosV[i]));
        }
    } break;
    case TableScanOutputType::MULTI_DATA_CHUNK: {
        for (auto& pos : info.outPosV) {
            localState.funcOutput.vectors.push_back(resultSet->getValueVector(pos).get());
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    // Init table function input.
    function::TableFunctionInitInput tableFunctionInitInput{info.bindData.get(), context->queryID};
    localState.funcState = info.function.initLocalStateFunc(tableFunctionInitInput,
        sharedState->funcState.get(), context->clientContext->getMemoryManager());
    localState.funcInput = function::TableFuncInput{info.bindData.get(), localState.funcState.get(),
        sharedState->funcState.get(), context->clientContext};
}

void TableFunctionCall::initGlobalStateInternal(ExecutionContext* ctx) {
    function::TableFunctionInitInput tableFunctionInitInput{info.bindData.get(), ctx->queryID};
    sharedState->funcState = info.function.initSharedStateFunc(tableFunctionInitInput);
}

bool TableFunctionCall::getNextTuplesInternal(ExecutionContext*) {
    localState.funcOutput.dataChunk.state->getSelVectorUnsafe().setSelSize(0);
    localState.funcOutput.dataChunk.resetAuxiliaryBuffer();
    for (auto i = 0u; i < localState.funcOutput.dataChunk.getNumValueVectors(); i++) {
        localState.funcOutput.dataChunk.getValueVectorMutable(i).setAllNonNull();
    }
    auto numTuplesScanned = info.function.tableFunc(localState.funcInput, localState.funcOutput);
    localState.funcOutput.dataChunk.state->getSelVectorUnsafe().setToUnfiltered(numTuplesScanned);
    metrics->numOutputTuple.increase(numTuplesScanned);
    return numTuplesScanned != 0;
}

void TableFunctionCall::finalizeInternal(ExecutionContext* context) {
    info.function.finalizeFunc(context, sharedState->funcState.get(), localState.funcState.get());
}

double TableFunctionCall::getProgress(ExecutionContext* /*context*/) const {
    return info.function.progressFunc(sharedState->funcState.get());
}

} // namespace processor
} // namespace kuzu
