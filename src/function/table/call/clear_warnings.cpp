#include "function/table/simple_table_functions.h"
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
    TableFuncBindInput*) {
    return std::make_unique<SimpleTableFuncBindData>(0);
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
