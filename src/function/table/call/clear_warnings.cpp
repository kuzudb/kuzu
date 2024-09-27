#include "function/table/call_functions.h"
#include "processor/warning_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput&) {
    RUNTIME_CHECK(auto sharedState = input.sharedState->ptrCast<CallFuncSharedState>();
                  auto morsel = sharedState->getMorsel(); KU_ASSERT(!morsel.hasMoreToOutput()););
    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    ScanTableFuncBindInput*) {
    context->getWarningContextUnsafe().clearPopulatedWarnings();

    static constexpr offset_t noReturnValueOffset = 0;
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    return std::make_unique<CallTableFuncBindData>(std::move(columnTypes), std::move(columnNames),
        noReturnValueOffset);
}

function_set ClearWarningsFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
