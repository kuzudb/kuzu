#include "binder/binder.h"
#include "function/table/bind_input.h"
#include "function/table/simple_table_functions.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

struct CurrentSettingBindData final : public SimpleTableFuncBindData {
    std::string result;

    CurrentSettingBindData(std::string result, binder::expression_vector columns,
        offset_t maxOffset)
        : SimpleTableFuncBindData{std::move(columns), maxOffset}, result{std::move(result)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CurrentSettingBindData>(result, columns, maxOffset);
    }
};

static common::offset_t tableFunc(TableFuncInput& data, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState = data.sharedState->ptrCast<SimpleTableFuncSharedState>();
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
    TableFuncBindInput* input) {
    auto optionName = input->getLiteralVal<std::string>(0);
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back(optionName);
    columnTypes.push_back(LogicalType::STRING());
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<CurrentSettingBindData>(
        context->getCurrentSetting(optionName).toString(), columns, 1 /* one row result */);
}

function_set CurrentSettingFunction::getFunctionSet() {
    function_set functionSet;
    auto function =
        std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = initSharedState;
    function->initLocalStateFunc = initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
