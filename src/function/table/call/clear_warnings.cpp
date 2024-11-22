#include "function/table/call_functions.h"
#include "processor/execution_context.h"
#include "processor/warning_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& /*output*/) {
    input.context->clientContext->getWarningContextUnsafe().clearPopulatedWarnings();
    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
    ScanTableFuncBindInput*) {
    return std::make_unique<StandaloneTableFuncBindData>();
}

function_set ClearWarningsFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState,
        initEmptyLocalState, std::vector<LogicalTypeID>{});
    func->canParallelFunc = []() { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace function
} // namespace kuzu
