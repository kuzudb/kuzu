#include "function/list/functions/list_to_string_function.h"

#include "common/type_utils.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void ListToString::operation(list_entry_t& input, ku_string_t& delim, ku_string_t& result,
    ValueVector& inputVector, ValueVector& /*delimVector*/, ValueVector& resultVector) {
    std::string resultStr = "";
    if (input.size != 0) {
        auto dataVector = ListVector::getDataVector(&inputVector);
        for (auto i = 0u; i < input.size - 1; i++) {
            resultStr += TypeUtils::entryToString(dataVector->dataType,
                ListVector::getListValuesWithOffset(&inputVector, input, i), dataVector);
            resultStr += delim.getAsString();
        }
        resultStr += TypeUtils::entryToString(dataVector->dataType,
            ListVector::getListValuesWithOffset(&inputVector, input, input.size - 1), dataVector);
    }
    StringVector::addString(&resultVector, result, resultStr);
}

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    std::vector<LogicalType> paramTypes;
    paramTypes.push_back(input.arguments[0]->getDataType().copy());
    paramTypes.push_back(LogicalType(input.definition->parameterTypeIDs[1]));
    return std::make_unique<FunctionBindData>(std::move(paramTypes), LogicalType::STRING());
}

function_set ListToStringFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        ScalarFunction::BinaryExecListStructFunction<list_entry_t, ku_string_t, ku_string_t,
            ListToString>);
    function->bindFunc = bindFunc;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
