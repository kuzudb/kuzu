#include "function/table/call_functions.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState = input.sharedState->ptrCast<CallFuncSharedState>();
    auto outputVector = dataChunk.getValueVector(0);
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        return 0;
    }
    auto pos = dataChunk.state->getSelVector()[0];
    outputVector->setValue(pos, std::string(KUZU_VERSION));
    outputVector->setNull(pos, false);
    return 1;
}

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext*, TableFuncBindInput*) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back("version");
    returnTypes.emplace_back(*LogicalType::STRING());
    return std::make_unique<CallTableFuncBindData>(std::move(returnTypes),
        std::move(returnColumnNames), 1 /* one row result */);
}

function_set DBVersionFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
