#include "binder/binder.h"
#include "function/table/simple_table_functions.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState = input.sharedState->ptrCast<SimpleTableFuncSharedState>();
    auto& outputVector = dataChunk.getValueVectorMutable(0);
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        return 0;
    }
    auto pos = dataChunk.state->getSelVector()[0];
    outputVector.setValue(pos, std::string(KUZU_VERSION));
    return 1;
}

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext*, TableFuncBindInput* input) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back("version");
    returnTypes.emplace_back(LogicalType::STRING());
    auto columns = input->binder->createVariables(returnColumnNames, returnTypes);
    return std::make_unique<SimpleTableFuncBindData>(std::move(columns), 1 /* one row result */);
}

function_set DBVersionFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = initSharedState;
    function->initLocalStateFunc = initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
