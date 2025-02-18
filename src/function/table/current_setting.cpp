#include "binder/binder.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/table_function.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

struct CurrentSettingBindData final : TableFuncBindData {
    std::string result;

    CurrentSettingBindData(std::string result, binder::expression_vector columns,
        offset_t maxOffset)
        : TableFuncBindData{std::move(columns), maxOffset}, result{std::move(result)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CurrentSettingBindData>(result, columns, maxOffset);
    }
};

static offset_t tableFunc(const TableFuncInput& data, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    const auto sharedState = data.sharedState->ptrCast<TableFuncSharedState>();
    auto& outputVector = dataChunk.getValueVectorMutable(0);
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        return 0;
    }
    const auto currentSettingBindData = data.bindData->constPtrCast<CurrentSettingBindData>();
    const auto pos = dataChunk.state->getSelVector()[0];
    outputVector.setValue(pos, currentSettingBindData->result);
    return 1;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const ClientContext* context,
    const TableFuncBindInput* input) {
    auto optionName = input->getLiteralVal<std::string>(0);
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back(optionName);
    columnTypes.push_back(LogicalType::STRING());
    columnNames = TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<CurrentSettingBindData>(
        context->getCurrentSetting(optionName).toString(), columns, 1 /* one row result */);
}

function_set CurrentSettingFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = TableFunction::initSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
