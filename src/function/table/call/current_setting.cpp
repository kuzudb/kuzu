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
        return std::make_unique<CurrentSettingBindData>(
            result, columnTypes, columnNames, maxOffset);
    }
};

static void tableFunc(TableFunctionInput& data, DataChunk& outChunk) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, CallFuncSharedState*>(data.sharedState);
    auto outputVector = outChunk.getValueVector(0);
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        outChunk.state->selVector->selectedSize = 0;
        return;
    }
    auto currentSettingBindData =
        ku_dynamic_cast<TableFuncBindData*, CurrentSettingBindData*>(data.bindData);
    auto pos = outChunk.state->selVector->selectedPositions[0];
    outputVector->setValue(pos, currentSettingBindData->result);
    outputVector->setNull(pos, false);
    outChunk.state->selVector->selectedSize = 1;
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    ClientContext* context, TableFuncBindInput* input) {
    auto optionName = input->inputs[0].getValue<std::string>();
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back(optionName);
    columnTypes.push_back(*LogicalType::STRING());
    return std::make_unique<CurrentSettingBindData>(
        context->getCurrentSetting(optionName).toString(), std::move(columnTypes),
        std::move(columnNames), 1 /* one row result */);
}

function_set CurrentSettingFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(CURRENT_SETTING_FUNC_NAME, tableFunc,
        bindFunc, initSharedState, initEmptyLocalState,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
