#include "function/table/call_functions.h"
#include "processor/warning_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {
static constexpr offset_t singleValueReturnOffset = 1;

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState = input.sharedState->ptrCast<CallFuncSharedState>();
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    // we should only be reporting a single row containing the status code
    KU_ASSERT(morsel.endOffset - morsel.startOffset == singleValueReturnOffset);
    static constexpr uint64_t statusCodeColumnIdx = 0;
    static constexpr uint8_t successStatusCode = 0;
    dataChunk.getValueVector(statusCodeColumnIdx)->setValue(morsel.startOffset, successStatusCode);
    return singleValueReturnOffset;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    ScanTableFuncBindInput*) {
    context->getWarningContextUnsafe().clearPopulatedWarnings();

    std::vector<std::string> columnNames({"status"});
    std::vector<LogicalType> columnTypes;
    columnTypes.push_back(LogicalType::UINT8());
    return std::make_unique<CallTableFuncBindData>(std::move(columnTypes), std::move(columnNames),
        singleValueReturnOffset);
}

function_set ClearWarningsFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
