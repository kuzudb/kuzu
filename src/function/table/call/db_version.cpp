#include "function/table/call_functions.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

static void tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, CallFuncSharedState*>(input.sharedState);
    auto outputVector = outputChunk.getValueVector(0);
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        outputChunk.state->selVector->selectedSize = 0;
        return;
    }
    auto pos = outputChunk.state->selVector->selectedPositions[0];
    outputVector->setValue(pos, std::string(KUZU_VERSION));
    outputVector->setNull(pos, false);
    outputChunk.state->selVector->selectedSize = 1;
}

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext*, TableFuncBindInput*) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back("version");
    returnTypes.emplace_back(*LogicalType::STRING());
    return std::make_unique<CallTableFuncBindData>(
        std::move(returnTypes), std::move(returnColumnNames), 1 /* one row result */);
}

function_set DBVersionFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(DB_VERSION_FUNC_NAME, tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
