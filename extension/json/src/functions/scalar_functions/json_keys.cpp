#include "function/scalar_function.h"
#include "json_scalar_functions.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    KU_ASSERT(input.arguments.size() == 1);
    auto type = input.arguments[0]->dataType.copy();
    if (type.getLogicalTypeID() == LogicalTypeID::ANY) {
        type = LogicalType::INT64();
    }
    auto bindData = std::make_unique<FunctionBindData>(LogicalType::LIST(LogicalType::STRING()));
    bindData->paramTypes.push_back(std::move(type));
    return bindData;
}

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    auto resultDataVector = ListVector::getDataVector(&result);
    resultDataVector->resetAuxiliaryBuffer();
    const auto& param = parameters[0];
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
        ++selectedPos) {
        auto resultPos = result.state->getSelVector()[selectedPos];
        auto paramPos = param->state->getSelVector()[param->state->isFlat() ? 0 : selectedPos];
        auto isNull = parameters[0]->isNull(paramPos);
        result.setNull(resultPos, isNull);
        if (!isNull) {
            auto paramStr = param->getValue<ku_string_t>(paramPos).getAsString();
            auto keys = jsonGetKeys(stringToJson(paramStr));
            auto resultList = ListVector::addList(&result, keys.size());
            result.setValue<list_entry_t>(resultPos, resultList);
            for (auto i = 0u; i < resultList.size; ++i) {
                StringVector::addString(resultDataVector, resultList.offset + i, keys[i]);
            }
        }
    }
}

function_set JsonKeysFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
            LogicalTypeID::LIST, execFunc, bindFunc));
    return result;
}

} // namespace json_extension
} // namespace kuzu
