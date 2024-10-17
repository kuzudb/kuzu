#include "function/table/call_functions.h"
#include "processor/warning_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& /*output*/) {
    input.context->getWarningContextUnsafe().clearPopulatedWarnings();
    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
    ScanTableFuncBindInput*) {
    return std::make_unique<StandaloneTableFuncBindData>();
}

function_set ClearWarningsFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
