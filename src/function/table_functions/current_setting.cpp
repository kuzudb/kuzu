#include "function/table_functions/current_setting.h"

#include "common/vector/value_vector.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"
#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {
struct CurrentSettingBindData : public TableFuncBindData {
    std::string result;

    CurrentSettingBindData(std::string result, std::vector<common::LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : result{std::move(result)}, TableFuncBindData{std::move(returnTypes),
                                         std::move(returnColumnNames), maxOffset} {}

    std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<CurrentSettingBindData>(
            result, returnTypes, returnColumnNames, maxOffset);
    }
};

void CurrentSettingFunction::tableFunc(std::pair<offset_t, offset_t> morsel,
    function::TableFuncBindData* bindData, std::vector<ValueVector*> outputVectors) {
    auto currentSettingBindData = reinterpret_cast<CurrentSettingBindData*>(bindData);
    auto outputVector = outputVectors[0];
    auto pos = outputVector->state->selVector->selectedPositions[0];
    outputVectors[0]->setValue(pos, currentSettingBindData->result);
    outputVectors[0]->setNull(pos, false);
    outputVector->state->selVector->selectedSize = 1;
}

std::unique_ptr<TableFuncBindData> CurrentSettingFunction::bindFunc(main::ClientContext* context,
    kuzu::function::TableFuncBindInput input, catalog::CatalogContent* catalog) {
    auto optionName = input.inputs[0].getValue<std::string>();
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back(optionName);
    returnTypes.emplace_back(LogicalTypeID::STRING);
    return std::make_unique<CurrentSettingBindData>(context->getCurrentSetting(optionName),
        std::move(returnTypes), std::move(returnColumnNames), 1 /* one row result */);
}
} // namespace function
} // namespace kuzu
