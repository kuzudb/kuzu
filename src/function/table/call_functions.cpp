#include "function/table/call_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

CallFuncMorsel CallFuncSharedState::getMorsel() {
    std::lock_guard lck{mtx};
    KU_ASSERT(curOffset <= maxOffset);
    if (curOffset == maxOffset) {
        return CallFuncMorsel::createInvalidMorsel();
    } else {
        auto numValuesToOutput = std::min(DEFAULT_VECTOR_CAPACITY, maxOffset - curOffset);
        curOffset += numValuesToOutput;
        return CallFuncMorsel{curOffset - numValuesToOutput, curOffset};
    }
}

std::unique_ptr<TableFuncSharedState> CallFunction::initSharedState(TableFunctionInitInput& input) {
    auto callTableFuncBindData =
        ku_dynamic_cast<TableFuncBindData*, CallTableFuncBindData*>(input.bindData);
    return std::make_unique<CallFuncSharedState>(callTableFuncBindData->maxOffset);
}

std::unique_ptr<TableFuncLocalState> CallFunction::initEmptyLocalState(
    kuzu::function::TableFunctionInitInput&, kuzu::function::TableFuncSharedState*,
    storage::MemoryManager*) {
    return std::make_unique<TableFuncLocalState>();
}

} // namespace function
} // namespace kuzu
