#include "function/table/bind_input.h"
#include "function/table/call_functions.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

struct CurrentSettingBindData final : public CallTableFuncBindData {
    std::string result;

    CurrentSettingBindData(std::string result, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          result{std::move(result)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CurrentSettingBindData>(result, LogicalType::copy(columnTypes),
            columnNames, maxOffset);
    }
};

static common::offset_t tableFunc(TableFuncInput& data, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState = data.sharedState->ptrCast<CallFuncSharedState>();
    auto& outputVector = dataChunk.getValueVectorMutable(0);
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        return 0;
    }
    auto currentSettingBindData = data.bindData->constPtrCast<CurrentSettingBindData>();
    auto pos = dataChunk.state->getSelVector()[0];
    outputVector.setValue(pos, currentSettingBindData->result);
    return 1;
}

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    ScanTableFuncBindInput* input) {
    auto optionName = input->inputs[0].getValue<std::string>();
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back(optionName);
    columnTypes.push_back(LogicalType::STRING());
    return std::make_unique<CurrentSettingBindData>(
        context->getCurrentSetting(optionName).toString(), std::move(columnTypes),
        std::move(columnNames), 1 /* one row result */);
}

function_set CurrentSettingFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
