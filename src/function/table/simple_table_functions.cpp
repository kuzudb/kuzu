#include "function/table/simple_table_functions.h"

#include "common/exception/binder.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

SimpleTableFuncSharedState::~SimpleTableFuncSharedState() = default;

SimpleTableFuncMorsel SimpleTableFuncSharedState::getMorsel() {
    std::lock_guard lck{mtx};
    KU_ASSERT(curOffset <= maxOffset);
    if (curOffset == maxOffset) {
        return SimpleTableFuncMorsel::createInvalidMorsel();
    } else {
        const auto numValuesToOutput = std::min(DEFAULT_VECTOR_CAPACITY, maxOffset - curOffset);
        curOffset += numValuesToOutput;
        return {curOffset - numValuesToOutput, curOffset};
    }
}

std::unique_ptr<TableFuncSharedState> SimpleTableFunction::initSharedState(
    const TableFunctionInitInput& input) {
    const auto bindData = ku_dynamic_cast<SimpleTableFuncBindData*>(input.bindData);
    return std::make_unique<SimpleTableFuncSharedState>(bindData->maxOffset);
}

std::unique_ptr<TableFuncLocalState> SimpleTableFunction::initEmptyLocalState(
    const TableFunctionInitInput&, TableFuncSharedState*, storage::MemoryManager*) {
    return std::make_unique<TableFuncLocalState>();
}

std::vector<std::string> SimpleTableFunction::extractYieldVariables(
    const std::vector<std::string>& names, std::vector<parser::YieldVariable> yieldVariables) {
    std::vector<std::string> variableNames;
    if (!yieldVariables.empty()) {
        if (yieldVariables.size() != names.size()) {
            throw common::BinderException{"Output variables must all appear in the yield clause."};
        }
        for (auto i = 0u; i < names.size(); i++) {
            if (names[i] != yieldVariables[i].name) {
                throw common::BinderException{common::stringFormat(
                    "Unknown table function output variable name: {}.", yieldVariables[i].name)};
            }
            auto variableName =
                yieldVariables[i].hasAlias() ? yieldVariables[i].alias : yieldVariables[i].name;
            variableNames.push_back(variableName);
        }
    } else {
        variableNames = names;
    }
    return variableNames;
}

} // namespace function
} // namespace kuzu
